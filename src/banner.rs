use std::env;
use std::process;

pub fn print_banner() {
    let pid = process::id();

    let node_name = env::var("AURA_NODE_NAME")
        .or_else(|_| env::var("HOSTNAME"))
        .or_else(|_| env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "AURA-UNKNOWN".to_string());

    let c_id = "\x1b[38;2;80;160;255m";
    let c_met = "\x1b[38;2;140;140;140m";
    let c_val = "\x1b[38;2;220;220;220m";
    let c_ok = "\x1b[38;2;0;255;150m";
    let c_brd = "\x1b[38;2;70;70;70m";
    let c_rst = "\x1b[0m";

    println!(
        "\n{c_brd}┌────────────────────────────────────────────────────────────────────────┐{c_rst}"
    );

    let left_part = format!("NODE: {}", node_name);
    let right_part = format!("PID: {}", pid);

    let header_content = format!("{:<50}{:>20}", left_part, right_part);

    println!(
        "{c_brd}│{c_rst} {c_met}{:<70} {c_brd}│{c_rst}",
        header_content
    );
    println!(
        "{c_brd}└────────────────────────────────────────────────────────────────────────┘{c_rst}"
    );

    let logo = r#"
 █████╗ ██╗   ██╗██████╗  █████╗       ███████╗██████╗ ██╗ ██████╗███████╗
██╔══██╗██║   ██║██╔══██╗██╔══██╗      ██╔════╝██╔══██╗██║██╔════╝██╔════╝
███████║██║   ██║██████╔╝███████║█████╗█████╗  ██████╔╝██║██║     ███████║
██╔══██║██║   ██║██╔══██╗██╔══██║╚════╝██╔══╝  ██╔═══╝ ██║██║     ╚════██║
██║  ██║╚██████╔╝██║  ██║██║  ██║      ███████╗██║     ██║╚██████╗███████║
╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝      ╚══════╝╚═╝     ╚═╝ ╚═════╝╚══════╝"#;
    println!("{c_id}{logo}{c_rst}");

    println!(
        "\n{c_id}>> SYSTEM:{c_rst} {c_val}An Archiving Engine for EPICS-based Control Systems{c_rst}\n"
    );

    println!(
        "{c_brd}┌────────────────────────────────────────────────────────────────────────┐{c_rst}"
    );

    let row = |label: &str, value: &str, tag: &str| {
        println!(
            "{c_brd}│{c_rst} {c_id}{:<12}{c_rst} {c_brd}│{c_rst} {c_val}{:<42}{c_rst}   {c_met}[{:^8}]{c_rst} {c_brd}│{c_rst}",
            label, value, tag
        );
    };

    row("FRAMEWORK", "Universal EPICS Archiving Node", "OSS");
    row("RUNTIME", "Rust Engine / Tokio / No-GC", "STABLE");
    row("METROLOGY", "Delta-of-Delta Lossless High-Freq", "STREAM");
    row("DB_ENGINE", "TimescaleDB / Hypertable / SQL", "TIME");
    row("NET_STACK", "High-Perf EPICS PVXS (V7)", "PVXS");

    println!(
        "{c_brd}└────────────────────────────────────────────────────────────────────────┘{c_rst}"
    );

    let version = env!("CARGO_PKG_VERSION");

    println!(
        "{c_ok}●{c_rst} {c_val}SYSTEM_READY{c_rst} {c_met}v{version} | MIT License | Auth: C. Krawiec{c_rst}\n"
    );
}
