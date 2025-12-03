use anyhow::{Context, Result};
use rusoto_core::Region;
use rusoto_ec2::{
    CancelSpotInstanceRequestsRequest, DescribeInstancesRequest,
    DescribeSpotInstanceRequestsRequest, Ec2, Ec2Client, RequestSpotInstancesRequest,
    RequestSpotLaunchSpecification, TerminateInstancesRequest,
};

use std::{collections::HashMap, thread, time::Duration};

pub mod ssh;

pub struct Machine {
    pub ssh: Option<ssh::Session>,
    pub instance_type: String,
    pub private_ip: String,
    pub public_ip: String,
    pub dns: String,
}

pub struct MachineSetup {
    instance_type: String,
    ami: String,
    setup: Box<dyn Fn(&mut ssh::Session) -> Result<()>>,
}

impl MachineSetup {
    pub fn new<F>(instance_type: &str, ami: &str, setup: F) -> Self
    where
        F: Fn(&mut ssh::Session) -> Result<()> + 'static,
    {
        MachineSetup {
            instance_type: instance_type.to_string(),
            ami: ami.to_string(),
            setup: Box::new(setup),
        }
    }
}

pub struct FlotillaBuilder {
    descriptors: HashMap<String, (MachineSetup, u32)>,
    max_duration: i64,
}

impl Default for FlotillaBuilder {
    fn default() -> Self {
        FlotillaBuilder {
            descriptors: Default::default(),
            max_duration: 60,
        }
    }
}

impl FlotillaBuilder {
    pub fn add_set(&mut self, name: &str, number: u32, setup: MachineSetup) {
        // TODO: What if name is already in use?
        self.descriptors.insert(name.to_string(), (setup, number));
    }

    pub fn set_max_duration(&mut self, hours: u8) {
        self.max_duration = hours as i64 * 60;
    }

    pub async fn run<F>(self, f: F) -> Result<()>
    where
        F: FnOnce(HashMap<String, Vec<Machine>>) -> Result<()>,
    {
        let ec2 = Ec2Client::new(Region::ApSouth1);

        let mut setup_fns = HashMap::new();
        // 1. Issue Spot Requests
        let mut spot_request_ids = vec![];
        let mut id_to_name = HashMap::new();
        for (name, (setup, number)) in self.descriptors {
            let mut launch = RequestSpotLaunchSpecification::default();
            launch.image_id = Some(setup.ami);
            launch.instance_type = Some(setup.instance_type);

            setup_fns.insert(name.clone(), setup.setup);

            launch.security_groups = Some(vec!["flotilla-sg".to_string()]);
            launch.key_name = Some("flotilla-key-pair".to_string());

            let mut req = RequestSpotInstancesRequest::default();
            req.instance_count = Some(i64::from(number));
            // req.block_duration_minutes = Some(self.max_duration);
            req.launch_specification = Some(launch);

            let res = ec2
                .request_spot_instances(req)
                .await
                .context(format!("Failed to request spot instances for {}", name))?;

            let res = res
                .spot_instance_requests
                .context("spot_instance_requests should always return spot instance requests.")?;

            spot_request_ids.extend(
                res.into_iter()
                    .filter_map(|sir| sir.spot_instance_request_id)
                    .map(|sir| {
                        id_to_name.insert(sir.clone(), name.clone());
                        sir
                    }),
            )
        }

        // 2. Wait for instances to come up
        let mut req = DescribeSpotInstanceRequestsRequest::default();
        req.spot_instance_request_ids = Some(spot_request_ids.clone());
        let instances: Vec<_>;
        let mut all_active;
        loop {
            let res = ec2
                .describe_spot_instance_requests(req.clone())
                .await
                .context("Failed to describe spot instances")?;

            let any_open = res.spot_instance_requests.as_ref().map_or(false, |v| {
                v.iter()
                    .any(|sir| sir.state.as_ref().map_or(false, |s| s == "open"))
            });

            if !any_open {
                all_active = true;
                instances = res
                    .spot_instance_requests
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|sir| {
                        if sir.state? == "active" {
                            let name = id_to_name
                                .remove(
                                    &sir.spot_instance_request_id
                                        .expect("spot instance must have spot instance request id"),
                                )
                                .expect("every spot request id is made of some machine set");
                            id_to_name.insert(sir.instance_id.clone()?, name);
                            sir.instance_id
                        } else {
                            all_active = false;
                            None
                        }
                    })
                    .collect();

                break;
            } else {
                thread::sleep(Duration::from_millis(500));
            }
        }

        // 3. Stop spot requests
        let mut cancel = CancelSpotInstanceRequestsRequest::default();
        cancel.spot_instance_request_ids = spot_request_ids;

        ec2.cancel_spot_instance_requests(cancel)
            .await
            .context("failed to cancel spot instances")?;

        // 4. Wait until all instances are up and setups have been run
        let mut machines: HashMap<String, Vec<Machine>> = HashMap::new();
        let mut desc_req = DescribeInstancesRequest::default();
        desc_req.instance_ids = Some(instances);
        let mut all_machine_are_ready = false;

        while !all_machine_are_ready {
            all_machine_are_ready = true;
            machines.clear();

            let reservations = ec2
                .describe_instances(desc_req.clone())
                .await
                .context("Failed to describe spot instances")?
                .reservations
                .unwrap_or_else(Vec::new);

            for reservation in reservations {
                for instance in reservation.instances.unwrap_or_else(Vec::new) {
                    let state = instance
                        .state
                        .as_ref()
                        .map(|s| s.name.as_deref().unwrap_or(""))
                        .unwrap();

                    if state != "running" {
                        all_machine_are_ready = false;
                        continue;
                    }

                    if instance.public_ip_address.is_none() {
                        all_machine_are_ready = false;
                        continue;
                    }

                    // println!("Instance state: {}", state);

                    let machine = Machine {
                        ssh: None,
                        instance_type: instance.instance_type.unwrap(),
                        private_ip: instance.private_ip_address.unwrap(),
                        public_ip: instance.public_ip_address.unwrap(),
                        dns: instance.public_dns_name.unwrap_or_default(),
                    };
                    let name = id_to_name[&instance.instance_id.unwrap()].clone();
                    machines.entry(name).or_insert_with(Vec::new).push(machine);
                }
            }
        }

        // TODO: Assert here that instances in each set is the same as requested.

        // 5. Once an instance is ready, run setup closure
        if all_active {
            for (name, machines) in &mut machines {
                let f = &setup_fns[name];
                for machine in machines {
                    let address = format!("{}:22", machine.public_ip);
                    println!("Waiting for SSH on {}...", address);
                    let mut sess =
                        ssh::Session::connect(&format!("{}:22", machine.public_ip)).context(
                            format!("Faield to ssh to {} machine {}", name, machine.public_ip),
                        )?;
                    f(&mut sess).context(format!("setup procedure for {} machine failed", name))?;
                    machine.ssh = Some(sess);
                }
            }
            // 5. Invoke F closures with machine descriptors
            f(machines).context("flotilla main routine failed")?;
        }

        // 6. Terminate all instances

        println!("Terminating Instances");
        let mut termination_req = TerminateInstancesRequest::default();
        termination_req.instance_ids = desc_req
            .instance_ids
            .clone()
            .expect("Go to Describe Instance Request");
        ec2.terminate_instances(termination_req)
            .await
            .context("Failed to terminate flotilla instances")?;

        Ok(())
    }
}
