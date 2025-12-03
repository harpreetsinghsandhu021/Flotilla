use flotilla::{FlotillaBuilder, Machine, MachineSetup};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let mut b = FlotillaBuilder::default();

    b.add_set(
        "server",
        1,
        MachineSetup::new("t3.micro", "ami-0912f71e06545ad88", |ssh| {
            ssh.cmd("date").map(|out| {
                println!("{}", out);
            })
        }),
    );

    b.add_set(
        "client",
        1,
        MachineSetup::new("t3.micro", "ami-0912f71e06545ad88", |ssh| {
            ssh.cmd("date").map(|out| {
                println!("{}", out);
            })
        }),
    );

    b.run(|vms: HashMap<String, Vec<Machine>>| {
        println!("{}", vms["server"][0].private_ip);
        println!("{}", vms["client"][0].private_ip);
        Ok(())
    })
    .await
    .unwrap();
}
