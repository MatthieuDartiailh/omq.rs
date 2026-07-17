use std::collections::BTreeMap;

use super::common::{
    C_LIBZMQ, C_LIBZMQ_2T, C_OMQ_1T, C_OMQ_2T, COMPARISON_SIZES, CpuData, Impl, ValMap,
    draw_multirow_throughput, load_tput, out_dir,
};

const FANIO_IMPLS: &[Impl] = &[
    Impl {
        key: "libzmq",
        label: "libzmq",
        threads: "1 IO",
        color: C_LIBZMQ,
    },
    Impl {
        key: "libzmq-2t",
        label: "libzmq",
        threads: "2 IO",
        color: C_LIBZMQ_2T,
    },
    Impl {
        key: "omq-tokio-1t",
        label: "omq",
        threads: "1 IO",
        color: C_OMQ_1T,
    },
    Impl {
        key: "omq-tokio-2t",
        label: "omq",
        threads: "2 IO",
        color: C_OMQ_2T,
    },
];

const PEER_COUNTS: &[u64] = &[4, 32];

fn generate_fanio(kind: &str, dir_name: &str, title_fn: &dyn Fn(u64) -> String) {
    let dir = out_dir();
    let sub = dir.join("pushpull").join(dir_name);
    std::fs::create_dir_all(&sub).ok();

    let mut panel_data: Vec<(u64, ValMap, ValMap, BTreeMap<String, CpuData>)> = Vec::new();
    for &peers in PEER_COUNTS {
        let (tput, msgs, cpu) = load_tput(kind, "tcp", Some(peers), FANIO_IMPLS);
        if !tput.is_empty() {
            panel_data.push((peers, tput, msgs, cpu));
        }
    }
    if panel_data.is_empty() {
        return;
    }

    let mut merged_cpu: BTreeMap<String, CpuData> = BTreeMap::new();
    for (_, _, _, cpu) in &panel_data {
        for (k, v) in cpu {
            merged_cpu.entry(k.clone()).or_insert_with(|| CpuData {
                sender: v.sender,
                receiver: v.receiver,
            });
        }
    }
    let rows: Vec<(u64, &ValMap, &ValMap)> =
        panel_data.iter().map(|(p, t, m, _)| (*p, t, m)).collect();
    let out = sub.join("tcp.svg");
    let chart_title = title_fn(0);
    let _ = kind;
    let (snd_label, rcv_label) = ("push CPU%", "pull CPU%");
    draw_multirow_throughput(
        &out,
        &chart_title,
        &rows,
        COMPARISON_SIZES,
        FANIO_IMPLS,
        &merged_cpu,
        &|peers| format!("{peers} peers"),
        snd_label,
        rcv_label,
    )
    .expect("draw fanio chart");
    eprintln!("Written: {}", out.display());
}

pub(crate) fn generate() {
    generate_fanio("fan_out", "fanout", &|_| {
        "PUSH fan-out, TCP loopback, 2-process".to_string()
    });
    generate_fanio("fan_in", "fanin", &|_| {
        "PUSH fan-in, TCP loopback, 2-process".to_string()
    });
}
