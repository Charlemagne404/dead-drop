use dead_drop_lib::test_support::{
    IncomingTransfer, TestEventSink, TestPeer, TransferPhase, TransferRun, TransferSnapshot,
};
use sha2::{Digest, Sha256};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    fs::{self, File},
    io::{Read, Write},
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
struct MeasuringAllocator;

static LIVE_ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static PEAK_ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: MeasuringAllocator = MeasuringAllocator;

unsafe impl GlobalAlloc for MeasuringAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE_ALLOCATIONS.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            if new_size >= layout.size() {
                record_allocation(new_size - layout.size());
            } else {
                LIVE_ALLOCATIONS.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_pointer
    }
}

fn record_allocation(size: usize) {
    let live = LIVE_ALLOCATIONS.fetch_add(size, Ordering::Relaxed) + size;
    let mut peak = PEAK_ALLOCATIONS.load(Ordering::Relaxed);
    while live > peak {
        match PEAK_ALLOCATIONS.compare_exchange_weak(
            peak,
            live,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => peak = observed,
        }
    }
}

fn start_allocation_probe() -> usize {
    let baseline = LIVE_ALLOCATIONS.load(Ordering::Relaxed);
    PEAK_ALLOCATIONS.store(baseline, Ordering::Relaxed);
    baseline
}

fn allocation_peak_delta(baseline: usize) -> usize {
    PEAK_ALLOCATIONS
        .load(Ordering::Relaxed)
        .saturating_sub(baseline)
}

async fn wait_incoming(events: &TestEventSink, transfer_id: &str) -> IncomingTransfer {
    timeout(TEST_TIMEOUT, events.wait_for_incoming(transfer_id))
        .await
        .expect("incoming request should arrive")
}

async fn wait_phase(
    events: &TestEventSink,
    transfer_id: &str,
    phase: TransferPhase,
) -> TransferSnapshot {
    timeout(TEST_TIMEOUT, events.wait_for_phase(transfer_id, phase))
        .await
        .expect("transfer phase should arrive")
}

async fn wait_terminal(events: &TestEventSink, transfer_id: &str) -> TransferSnapshot {
    timeout(TEST_TIMEOUT, events.wait_for_terminal(transfer_id))
        .await
        .expect("transfer should reach a terminal phase")
}

async fn wait_run(run: TransferRun) {
    timeout(TEST_TIMEOUT, run.wait())
        .await
        .expect("transfer task should finish")
        .expect("transfer task should not panic");
}

async fn wait_idle(left: &TestPeer, right: &TestPeer) {
    timeout(TEST_TIMEOUT, async {
        tokio::join!(left.wait_until_idle(), right.wait_until_idle());
    })
    .await
    .expect("both peers should return to idle");
}

fn write_file(path: &Path, contents: &[u8]) {
    fs::write(path, contents).expect("test file should be written");
}

fn write_pattern_file(path: &Path, size: usize, seed: u64) -> String {
    let mut file = File::create(path).expect("pattern file should be created");
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 8192];
    let mut offset = 0_u64;
    while offset < size as u64 {
        let count = (size as u64 - offset).min(buffer.len() as u64) as usize;
        for (index, byte) in buffer[..count].iter_mut().enumerate() {
            let value = offset + index as u64;
            *byte = (value.wrapping_mul(6364136223846793005).wrapping_add(seed) >> 29) as u8;
        }
        file.write_all(&buffer[..count])
            .expect("pattern file should be written");
        hasher.update(&buffer[..count]);
        offset += count as u64;
    }
    format!("{:x}", hasher.finalize())
}

fn sha256_file(path: &Path) -> String {
    let mut file = File::open(path).expect("received file should be readable");
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let count = file
            .read(&mut buffer)
            .expect("received file should be read");
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    format!("{:x}", hasher.finalize())
}

fn assert_no_part_files(directory: &Path) {
    let part_files: Vec<_> = fs::read_dir(directory)
        .expect("destination should be readable")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".part"))
        .collect();
    assert!(
        part_files.is_empty(),
        "temporary files remain: {:?}",
        part_files
            .iter()
            .map(|entry| entry.file_name())
            .collect::<Vec<_>>()
    );
}

fn distinct_phases(snapshots: &[TransferSnapshot]) -> Vec<TransferPhase> {
    let mut phases = Vec::new();
    for phase in snapshots.iter().map(|snapshot| snapshot.phase) {
        if phases.last() != Some(&phase) {
            phases.push(phase);
        }
    }
    phases
}

fn assert_phase_order(snapshots: &[TransferSnapshot], expected: &[TransferPhase]) {
    let actual = distinct_phases(snapshots);
    let mut cursor = 0;
    for phase in expected {
        let Some(index) = actual[cursor..]
            .iter()
            .position(|candidate| candidate == phase)
        else {
            panic!("phase {phase:?} missing from {actual:?}");
        };
        cursor += index + 1;
    }
}

async fn settle_simultaneous_transfers(
    a: &TestPeer,
    b: &TestPeer,
    a_transfer_id: &str,
    b_transfer_id: &str,
) {
    timeout(TEST_TIMEOUT, async {
        let mut a_incoming_handled = false;
        let mut b_incoming_handled = false;
        let mut a_done = false;
        let mut b_done = false;
        while !a_done || !b_done {
            tokio::select! {
                incoming = a.events.wait_for_incoming(b_transfer_id), if !a_incoming_handled => {
                    a_incoming_handled = true;
                    a.decline(&incoming.id).expect("A should resolve the simultaneous request");
                }
                incoming = b.events.wait_for_incoming(a_transfer_id), if !b_incoming_handled => {
                    b_incoming_handled = true;
                    b.decline(&incoming.id).expect("B should resolve the simultaneous request");
                }
                terminal = a.events.wait_for_terminal(a_transfer_id), if !a_done => {
                    assert_eq!(terminal.phase, TransferPhase::Rejected);
                    a_done = true;
                }
                terminal = b.events.wait_for_terminal(b_transfer_id), if !b_done => {
                    assert_eq!(terminal.phase, TransferPhase::Rejected);
                    b_done = true;
                }
            }
        }
    })
    .await
    .expect("simultaneous transfers should settle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn transfers_successfully_over_real_tcp_in_both_directions() {
    let a = TestPeer::new("Peer A");
    let b = TestPeer::new("Peer B");
    assert_ne!(a.device_id(), b.device_id());
    assert_ne!(a.address(), b.address());

    let source = a.source_dir().join("payload.txt");
    let contents = b"real socket transfer";
    write_file(&source, contents);
    let expected_hash = format!("{:x}", Sha256::digest(contents));

    let run = a
        .start_send_to(&b, vec![source.clone()])
        .expect("A should start a transfer");
    let id = run.id().to_string();
    let incoming = wait_incoming(&b.events, &id).await;
    assert_eq!(incoming.from.id, a.device_id());
    assert_eq!(incoming.files[0].name, "payload.txt");
    assert_eq!(incoming.files[0].size, contents.len() as u64);
    assert_eq!(incoming.files[0].sha256, expected_hash);
    b.accept(&id).expect("B should accept the transfer");
    wait_run(run).await;
    let sender_terminal = wait_terminal(&a.events, &id).await;
    let receiver_terminal = wait_terminal(&b.events, &id).await;
    assert_eq!(sender_terminal.phase, TransferPhase::Completed);
    assert_eq!(receiver_terminal.phase, TransferPhase::Completed);
    assert_eq!(
        fs::read(b.destination_dir().join("payload.txt")).unwrap(),
        contents
    );
    assert_no_part_files(&b.destination_dir());
    assert!(a
        .events
        .snapshots(&id)
        .iter()
        .any(|snapshot| snapshot.transferred_bytes > 0));
    assert_phase_order(
        &a.events.snapshots(&id),
        &[
            TransferPhase::Preparing,
            TransferPhase::Requesting,
            TransferPhase::WaitingForAcceptance,
            TransferPhase::Accepted,
            TransferPhase::Transferring,
            TransferPhase::Verifying,
            TransferPhase::Completing,
            TransferPhase::Completed,
        ],
    );
    wait_idle(&a, &b).await;

    let reverse_source = b.source_dir().join("reverse.dat");
    let reverse_contents = b"reverse direction";
    write_file(&reverse_source, reverse_contents);
    let reverse = b
        .start_send_to(&a, vec![reverse_source])
        .expect("B should start a reverse transfer");
    let reverse_id = reverse.id().to_string();
    wait_incoming(&a.events, &reverse_id).await;
    a.accept(&reverse_id)
        .expect("A should accept the reverse transfer");
    wait_run(reverse).await;
    assert_eq!(
        wait_terminal(&b.events, &reverse_id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&a.events, &reverse_id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        fs::read(a.destination_dir().join("reverse.dat")).unwrap(),
        reverse_contents
    );
    assert_no_part_files(&a.destination_dir());
    wait_idle(&a, &b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn first_contact_requires_trust_and_trusted_identity_reconnects() {
    let a = TestPeer::new("Peer A");
    let b = TestPeer::new("Peer B");
    a.disable_auto_trust();
    b.disable_auto_trust();

    let source = a.source_dir().join("first-contact.txt");
    write_file(&source, b"first contact");
    let a_requests_before = a.events.trust_requests().len();
    let b_requests_before = b.events.trust_requests().len();
    let run = a
        .start_send_to_untrusted(&b, vec![source.clone()])
        .expect("first-contact transfer should start");
    let id = run.id().to_string();

    let a_request = timeout(
        TEST_TIMEOUT,
        a.events.wait_for_trust_request_after(a_requests_before),
    )
    .await
    .expect("sender should show a first-contact trust request");
    assert_eq!(a_request.reason, "new_device");
    assert_eq!(a_request.device.fingerprint, b.identity().fingerprint);
    assert!(a_request.short_fingerprint.contains('…'));
    a.respond_to_trust(&a_request.id, true)
        .expect("sender trust decision should resolve");

    let b_request = timeout(
        TEST_TIMEOUT,
        b.events.wait_for_trust_request_after(b_requests_before),
    )
    .await
    .expect("receiver should show a first-contact trust request");
    assert_eq!(b_request.reason, "new_device");
    assert_eq!(b_request.device.fingerprint, a.identity().fingerprint);
    b.respond_to_trust(&b_request.id, true)
        .expect("receiver trust decision should resolve");

    wait_incoming(&b.events, &id).await;
    b.accept(&id).expect("receiver should accept first contact");
    wait_run(run).await;
    assert_eq!(
        wait_terminal(&a.events, &id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&b.events, &id).await.phase,
        TransferPhase::Completed
    );
    wait_idle(&a, &b).await;

    let a_requests_before_reconnect = a.events.trust_requests().len();
    let b_requests_before_reconnect = b.events.trust_requests().len();
    let reconnect_source = a.source_dir().join("trusted-reconnect.txt");
    write_file(&reconnect_source, b"trusted reconnect");
    let reconnect = a
        .start_send_to_untrusted(&b, vec![reconnect_source])
        .expect("trusted reconnect should start");
    let reconnect_id = reconnect.id().to_string();
    let incoming = wait_incoming(&b.events, &reconnect_id).await;
    assert_eq!(incoming.from.fingerprint, a.identity().fingerprint);
    assert_eq!(a.events.trust_requests().len(), a_requests_before_reconnect);
    assert_eq!(b.events.trust_requests().len(), b_requests_before_reconnect);
    b.accept(&reconnect_id)
        .expect("receiver should accept trusted reconnect");
    wait_run(reconnect).await;
    assert_eq!(
        wait_terminal(&a.events, &reconnect_id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&b.events, &reconnect_id).await.phase,
        TransferPhase::Completed
    );
    wait_idle(&a, &b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn multiple_files_preserve_metadata_order_and_hashes() {
    let a = TestPeer::new("Sender");
    let b = TestPeer::new("Receiver");
    let names = ["first.txt", "archive.tar.gz", "third.no-extension"];
    let mut paths = Vec::new();
    for (index, name) in names.iter().enumerate() {
        let path = a.source_dir().join(name);
        write_file(&path, format!("file-{index}").as_bytes());
        paths.push(path);
    }

    let run = a.start_send_to(&b, paths).expect("batch should start");
    let id = run.id().to_string();
    let incoming = wait_incoming(&b.events, &id).await;
    assert_eq!(
        incoming
            .files
            .iter()
            .map(|file| file.name.as_str())
            .collect::<Vec<_>>(),
        names
    );
    assert_eq!(
        incoming.total_bytes,
        incoming.files.iter().map(|file| file.size).sum::<u64>()
    );
    b.accept(&id).expect("receiver should accept batch");
    wait_run(run).await;
    assert_eq!(
        wait_terminal(&a.events, &id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&b.events, &id).await.phase,
        TransferPhase::Completed
    );
    for (index, name) in names.iter().enumerate() {
        let destination = b.destination_dir().join(name);
        assert_eq!(
            fs::read(&destination).unwrap(),
            format!("file-{index}").into_bytes()
        );
        assert_eq!(sha256_file(&destination), incoming.files[index].sha256);
    }
    assert_no_part_files(&b.destination_dir());
    wait_idle(&a, &b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn zero_byte_files_finalize_and_report_completion() {
    let a = TestPeer::new("Sender");
    let b = TestPeer::new("Receiver");
    let source = a.source_dir().join("empty");
    File::create(&source).expect("zero-byte source should be created");
    let run = a
        .start_send_to(&b, vec![source])
        .expect("zero-byte transfer should start");
    let id = run.id().to_string();
    let incoming = wait_incoming(&b.events, &id).await;
    assert_eq!(incoming.total_bytes, 0);
    assert_eq!(incoming.files[0].size, 0);
    b.accept(&id)
        .expect("receiver should accept zero-byte file");
    wait_run(run).await;
    assert_eq!(
        wait_terminal(&a.events, &id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&b.events, &id).await.phase,
        TransferPhase::Completed
    );
    assert!(b.destination_dir().join("empty").is_file());
    assert_eq!(fs::read(b.destination_dir().join("empty")).unwrap(), b"");
    assert_no_part_files(&b.destination_dir());
    wait_idle(&a, &b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn large_file_streams_with_bounded_buffers_and_verified_checksum() {
    let a = TestPeer::new("Sender");
    let b = TestPeer::new("Receiver");
    let source = a.source_dir().join("large-stream.bin");
    let size = 32 * 1024 * 1024;
    let expected_hash = write_pattern_file(&source, size, 0xdead_beef);
    let allocation_baseline = start_allocation_probe();
    let run = a
        .start_send_to(&b, vec![source])
        .expect("large transfer should start");
    let id = run.id().to_string();
    let incoming = wait_incoming(&b.events, &id).await;
    assert_eq!(incoming.files[0].size, size as u64);
    b.accept(&id)
        .expect("receiver should accept large transfer");
    wait_run(run).await;
    assert_eq!(
        wait_terminal(&a.events, &id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&b.events, &id).await.phase,
        TransferPhase::Completed
    );
    let destination = b.destination_dir().join("large-stream.bin");
    assert_eq!(fs::metadata(&destination).unwrap().len(), size as u64);
    assert_eq!(sha256_file(&destination), expected_hash);
    let peak_delta = allocation_peak_delta(allocation_baseline);
    assert!(
        peak_delta < 8 * 1024 * 1024,
        "streaming transfer allocated {peak_delta} bytes above its baseline"
    );
    assert!(
        a.events
            .snapshots(&id)
            .iter()
            .filter(|snapshot| snapshot.phase == TransferPhase::Transferring)
            .map(|snapshot| snapshot.transferred_bytes)
            .max()
            .unwrap_or_default()
            >= size as u64
    );
    assert_no_part_files(&b.destination_dir());
    wait_idle(&a, &b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn unicode_and_edge_case_filenames_survive_the_transfer() {
    let a = TestPeer::new("Sender");
    let b = TestPeer::new("Receiver");
    let names = [
        "space name.txt",
        "å ä ö.txt",
        "café résumé.txt",
        "東京の資料.txt",
        "emoji 🚀.bin",
        "archive.tar.gz",
        "no-extension",
        ".env",
    ];
    let mut paths = Vec::new();
    for (index, name) in names.iter().enumerate() {
        let path = a.source_dir().join(name);
        write_file(&path, format!("unicode-{index}").as_bytes());
        paths.push(path);
    }
    let run = a
        .start_send_to(&b, paths)
        .expect("unicode batch should start");
    let id = run.id().to_string();
    let incoming = wait_incoming(&b.events, &id).await;
    assert_eq!(
        incoming
            .files
            .iter()
            .map(|file| file.name.as_str())
            .collect::<Vec<_>>(),
        names
    );
    b.accept(&id).expect("receiver should accept unicode batch");
    wait_run(run).await;
    assert_eq!(
        wait_terminal(&a.events, &id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&b.events, &id).await.phase,
        TransferPhase::Completed
    );
    for (index, name) in names.iter().enumerate() {
        assert_eq!(
            fs::read(b.destination_dir().join(name)).unwrap(),
            format!("unicode-{index}").into_bytes()
        );
    }
    assert_no_part_files(&b.destination_dir());
    wait_idle(&a, &b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn existing_collisions_never_overwrite_and_use_the_next_name() {
    let a = TestPeer::new("Sender");
    let b = TestPeer::new("Receiver");
    for (name, contents) in [
        ("collision.txt", b"original".as_slice()),
        ("collision (1).txt", b"first collision".as_slice()),
        ("collision (2).txt", b"second collision".as_slice()),
    ] {
        write_file(&b.destination_dir().join(name), contents);
    }
    let source = a.source_dir().join("collision.txt");
    write_file(&source, b"incoming content");
    let run = a
        .start_send_to(&b, vec![source])
        .expect("collision transfer should start");
    let id = run.id().to_string();
    wait_incoming(&b.events, &id).await;
    b.accept(&id)
        .expect("receiver should accept collision transfer");
    wait_run(run).await;
    assert_eq!(
        wait_terminal(&a.events, &id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&b.events, &id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        fs::read(b.destination_dir().join("collision.txt")).unwrap(),
        b"original"
    );
    assert_eq!(
        fs::read(b.destination_dir().join("collision (1).txt")).unwrap(),
        b"first collision"
    );
    assert_eq!(
        fs::read(b.destination_dir().join("collision (2).txt")).unwrap(),
        b"second collision"
    );
    assert_eq!(
        fs::read(b.destination_dir().join("collision (3).txt")).unwrap(),
        b"incoming content"
    );
    assert_no_part_files(&b.destination_dir());
    wait_idle(&a, &b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn receiver_decline_rejects_cleanly_without_artifacts() {
    let a = TestPeer::new("Sender");
    let b = TestPeer::new("Receiver");
    let source = a.source_dir().join("declined.txt");
    write_file(&source, b"should not arrive");
    let run = a
        .start_send_to(&b, vec![source])
        .expect("transfer should start");
    let id = run.id().to_string();
    wait_incoming(&b.events, &id).await;
    b.decline(&id).expect("receiver should decline");
    wait_run(run).await;
    assert_eq!(
        wait_terminal(&a.events, &id).await.phase,
        TransferPhase::Rejected
    );
    assert_eq!(
        wait_terminal(&b.events, &id).await.phase,
        TransferPhase::Rejected
    );
    assert!(!b.destination_dir().join("declined.txt").exists());
    assert_no_part_files(&b.destination_dir());
    wait_idle(&a, &b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sender_cancellation_is_clean_while_waiting_early_and_near_completion() {
    let a = TestPeer::new("Sender");
    let b = TestPeer::new("Receiver");
    let source = a.source_dir().join("waiting-cancel.bin");
    write_pattern_file(&source, 2 * 96 * 1024, 1);
    let waiting = a
        .start_send_to(&b, vec![source])
        .expect("transfer should start");
    let waiting_id = waiting.id().to_string();
    wait_phase(&a.events, &waiting_id, TransferPhase::WaitingForAcceptance).await;
    wait_incoming(&b.events, &waiting_id).await;
    a.cancel(&waiting_id)
        .expect("sender cancellation should be accepted");
    wait_run(waiting).await;
    assert_eq!(
        wait_terminal(&a.events, &waiting_id).await.phase,
        TransferPhase::Canceled
    );
    assert_eq!(
        wait_terminal(&b.events, &waiting_id).await.phase,
        TransferPhase::Canceled
    );
    assert_no_part_files(&b.destination_dir());
    wait_idle(&a, &b).await;

    let source = a.source_dir().join("early-cancel.bin");
    write_pattern_file(&source, 4 * 96 * 1024, 2);
    a.events.pause_on_first_data_progress();
    let early = a
        .start_send_to(&b, vec![source])
        .expect("early cancel should start");
    let early_id = early.id().to_string();
    wait_incoming(&b.events, &early_id).await;
    b.accept(&early_id)
        .expect("receiver should accept early cancel");
    timeout(TEST_TIMEOUT, a.events.wait_until_paused())
        .await
        .expect("sender should reach early progress barrier");
    a.cancel(&early_id).expect("sender should cancel early");
    a.events.release_pause();
    wait_run(early).await;
    assert_eq!(
        wait_terminal(&a.events, &early_id).await.phase,
        TransferPhase::Canceled
    );
    assert_eq!(
        wait_terminal(&b.events, &early_id).await.phase,
        TransferPhase::Canceled
    );
    assert_no_part_files(&b.destination_dir());
    wait_idle(&a, &b).await;

    let source = a.source_dir().join("near-end-cancel.bin");
    write_pattern_file(&source, 2 * 96 * 1024, 3);
    a.events.pause_on_final_data_progress();
    let near_end = a
        .start_send_to(&b, vec![source])
        .expect("near-end cancel should start");
    let near_end_id = near_end.id().to_string();
    wait_incoming(&b.events, &near_end_id).await;
    b.accept(&near_end_id)
        .expect("receiver should accept near-end cancel");
    timeout(TEST_TIMEOUT, a.events.wait_until_paused())
        .await
        .expect("sender should reach final progress barrier");
    a.cancel(&near_end_id)
        .expect("sender should cancel near the end");
    a.events.release_pause();
    wait_run(near_end).await;
    assert_eq!(
        wait_terminal(&a.events, &near_end_id).await.phase,
        TransferPhase::Canceled
    );
    assert_eq!(
        wait_terminal(&b.events, &near_end_id).await.phase,
        TransferPhase::Canceled
    );
    assert!(!b.destination_dir().join("near-end-cancel.bin").exists());
    assert_no_part_files(&b.destination_dir());
    wait_idle(&a, &b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn receiver_cancellation_is_clean_early_and_near_completion() {
    let a = TestPeer::new("Sender");
    let b = TestPeer::new("Receiver");
    let source = a.source_dir().join("receiver-early.bin");
    write_pattern_file(&source, 4 * 96 * 1024, 4);
    b.events.pause_on_first_data_progress();
    let early = a
        .start_send_to(&b, vec![source])
        .expect("receiver cancel should start");
    let early_id = early.id().to_string();
    wait_incoming(&b.events, &early_id).await;
    b.accept(&early_id)
        .expect("receiver should accept early cancellation");
    timeout(TEST_TIMEOUT, b.events.wait_until_paused())
        .await
        .expect("receiver should reach early progress barrier");
    b.cancel(&early_id).expect("receiver should cancel early");
    b.events.release_pause();
    wait_run(early).await;
    assert_ne!(
        wait_terminal(&a.events, &early_id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&b.events, &early_id).await.phase,
        TransferPhase::Canceled
    );
    assert_no_part_files(&b.destination_dir());
    wait_idle(&a, &b).await;

    let source = a.source_dir().join("receiver-near-end.bin");
    write_pattern_file(&source, 2 * 96 * 1024, 5);
    a.events.pause_on_final_data_progress();
    let near_end = a
        .start_send_to(&b, vec![source])
        .expect("receiver near-end cancel should start");
    let near_end_id = near_end.id().to_string();
    wait_incoming(&b.events, &near_end_id).await;
    b.accept(&near_end_id)
        .expect("receiver should accept near-end cancellation");
    timeout(TEST_TIMEOUT, a.events.wait_until_paused())
        .await
        .expect("sender should reach final progress barrier");
    b.cancel(&near_end_id)
        .expect("receiver should cancel near the end");
    a.events.release_pause();
    wait_run(near_end).await;
    assert_ne!(
        wait_terminal(&a.events, &near_end_id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&b.events, &near_end_id).await.phase,
        TransferPhase::Canceled
    );
    assert!(!b.destination_dir().join("receiver-near-end.bin").exists());
    assert_no_part_files(&b.destination_dir());
    wait_idle(&a, &b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shutdown_during_transfer_cleans_staged_files_on_both_sides() {
    let mut a = TestPeer::new("Sender");
    let mut b = TestPeer::new("Receiver");
    let source = a.source_dir().join("shutdown.bin");
    write_pattern_file(&source, 4 * 96 * 1024, 6);
    b.events.pause_on_first_data_progress();
    let receiver_shutdown = a
        .start_send_to(&b, vec![source.clone()])
        .expect("receiver shutdown transfer should start");
    let receiver_shutdown_id = receiver_shutdown.id().to_string();
    wait_incoming(&b.events, &receiver_shutdown_id).await;
    b.accept(&receiver_shutdown_id)
        .expect("receiver should accept before shutdown");
    timeout(TEST_TIMEOUT, b.events.wait_until_paused())
        .await
        .expect("receiver should be actively receiving");
    b.shutdown().await;
    b.events.release_pause();
    wait_run(receiver_shutdown).await;
    assert_ne!(
        wait_terminal(&a.events, &receiver_shutdown_id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&b.events, &receiver_shutdown_id).await.phase,
        TransferPhase::Canceled
    );
    assert_no_part_files(&b.destination_dir());
    assert!(!b.destination_dir().join("shutdown.bin").exists());

    let receiver = TestPeer::new("Second Receiver");
    let sender_shutdown = a
        .start_send_to(&receiver, vec![source])
        .expect("sender shutdown transfer should start");
    let sender_shutdown_id = sender_shutdown.id().to_string();
    wait_incoming(&receiver.events, &sender_shutdown_id).await;
    receiver
        .accept(&sender_shutdown_id)
        .expect("receiver should accept before sender shutdown");
    a.events.pause_on_first_data_progress();
    timeout(TEST_TIMEOUT, a.events.wait_until_paused())
        .await
        .expect("sender should be actively sending");
    a.shutdown().await;
    a.events.release_pause();
    wait_run(sender_shutdown).await;
    assert_ne!(
        wait_terminal(&a.events, &sender_shutdown_id).await.phase,
        TransferPhase::Completed
    );
    assert_ne!(
        wait_terminal(&receiver.events, &sender_shutdown_id)
            .await
            .phase,
        TransferPhase::Completed
    );
    assert_no_part_files(&receiver.destination_dir());
    assert!(!receiver.destination_dir().join("shutdown.bin").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn disconnects_during_negotiation_active_transfer_and_completion_do_not_hang() {
    let receiver = TestPeer::new("Receiver");
    let mut negotiation = TcpStream::connect(receiver.address())
        .await
        .expect("negotiation socket should connect");
    negotiation
        .shutdown()
        .await
        .expect("negotiation socket should close");
    let mut response = Vec::new();
    timeout(TEST_TIMEOUT, negotiation.read_to_end(&mut response))
        .await
        .expect("negotiation disconnect should be observed")
        .expect("negotiation disconnect should be readable");
    assert!(receiver.is_idle());

    let mut receiver = TestPeer::new("Dropper");
    receiver.events.pause_on_first_data_progress();
    let sender = TestPeer::new("Sender");
    let source = sender.source_dir().join("active-disconnect.bin");
    write_pattern_file(&source, 4 * 96 * 1024, 7);
    let active = sender
        .start_send_to(&receiver, vec![source])
        .expect("active disconnect transfer should start");
    let active_id = active.id().to_string();
    wait_incoming(&receiver.events, &active_id).await;
    receiver
        .accept(&active_id)
        .expect("receiver should accept active transfer");
    timeout(TEST_TIMEOUT, receiver.events.wait_until_paused())
        .await
        .expect("receiver should reach the active transfer barrier");
    receiver.state().shutdown();
    receiver.events.release_pause();
    wait_run(active).await;
    assert_eq!(
        wait_terminal(&sender.events, &active_id).await.phase,
        TransferPhase::Failed
    );
    assert!(sender.is_idle());
    receiver.shutdown().await;

    let mut receiver = TestPeer::new("Completion Dropper");
    receiver.events.pause_on_phase(TransferPhase::Completing);
    let sender = TestPeer::new("Completion Sender");
    let source = sender.source_dir().join("completion-disconnect.txt");
    write_file(&source, b"drop after complete");
    let completion = sender
        .start_send_to(&receiver, vec![source])
        .expect("completion disconnect transfer should start");
    let completion_id = completion.id().to_string();
    wait_incoming(&receiver.events, &completion_id).await;
    receiver
        .accept(&completion_id)
        .expect("receiver should accept completion transfer");
    timeout(TEST_TIMEOUT, receiver.events.wait_until_paused())
        .await
        .expect("receiver should reach the completion barrier");
    receiver.state().shutdown();
    receiver.events.release_pause();
    wait_run(completion).await;
    assert_eq!(
        wait_terminal(&sender.events, &completion_id).await.phase,
        TransferPhase::Failed
    );
    assert!(sender.is_idle());
    receiver.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn busy_and_simultaneous_send_behavior_is_controlled_and_deadlock_free() {
    let a = TestPeer::new("A");
    let b = TestPeer::new("B");
    let c = TestPeer::new("C");
    let first_source = a.source_dir().join("busy-first.bin");
    write_pattern_file(&first_source, 4 * 96 * 1024, 8);
    a.events.pause_on_first_data_progress();
    let first = a
        .start_send_to(&b, vec![first_source])
        .expect("first transfer should start");
    let first_id = first.id().to_string();
    wait_incoming(&b.events, &first_id).await;
    b.accept(&first_id).expect("B should accept first transfer");
    timeout(TEST_TIMEOUT, a.events.wait_until_paused())
        .await
        .expect("first transfer should be active");
    let local_busy_source = a.source_dir().join("local-busy.txt");
    write_file(&local_busy_source, b"busy");
    let local_error = a
        .start_send_to(&b, vec![local_busy_source])
        .expect_err("sender should reject a second local transfer");
    assert!(local_error.contains("active transfer"));

    let remote_busy_source = c.source_dir().join("remote-busy.txt");
    write_file(&remote_busy_source, b"busy receiver");
    let remote_busy = c
        .start_send_to(&b, vec![remote_busy_source])
        .expect("busy receiver request should reach B");
    let remote_busy_id = remote_busy.id().to_string();
    wait_run(remote_busy).await;
    assert_eq!(
        wait_terminal(&c.events, &remote_busy_id).await.phase,
        TransferPhase::Rejected
    );
    a.cancel(&first_id)
        .expect("first transfer should be cancellable");
    a.events.release_pause();
    wait_run(first).await;
    assert_eq!(
        wait_terminal(&a.events, &first_id).await.phase,
        TransferPhase::Canceled
    );
    assert_eq!(
        wait_terminal(&b.events, &first_id).await.phase,
        TransferPhase::Canceled
    );
    wait_idle(&a, &b).await;

    let simultaneous_a_source = a.source_dir().join("simultaneous-a.txt");
    let simultaneous_b_source = b.source_dir().join("simultaneous-b.txt");
    write_file(&simultaneous_a_source, b"A to B");
    write_file(&simultaneous_b_source, b"B to A");
    let simultaneous_a = a
        .start_send_to(&b, vec![simultaneous_a_source])
        .expect("A simultaneous transfer should start");
    let simultaneous_a_id = simultaneous_a.id().to_string();
    let simultaneous_b = b
        .start_send_to(&a, vec![simultaneous_b_source])
        .expect("B simultaneous transfer should start");
    let simultaneous_b_id = simultaneous_b.id().to_string();
    settle_simultaneous_transfers(&a, &b, &simultaneous_a_id, &simultaneous_b_id).await;
    wait_run(simultaneous_a).await;
    wait_run(simultaneous_b).await;
    assert_eq!(
        wait_terminal(&a.events, &simultaneous_a_id).await.phase,
        TransferPhase::Rejected
    );
    assert_eq!(
        wait_terminal(&b.events, &simultaneous_b_id).await.phase,
        TransferPhase::Rejected
    );
    assert!(!a.destination_dir().join("simultaneous-b.txt").exists());
    assert!(!b.destination_dir().join("simultaneous-a.txt").exists());
    wait_idle(&a, &b).await;
}

async fn assert_malformed_secure_handshake_is_closed(
    peer: &TestPeer,
    client_preface: &[u8],
    suffix: &[u8],
) {
    let mut stream = TcpStream::connect(peer.address())
        .await
        .expect("malformed peer socket should connect");
    stream
        .write_all(client_preface)
        .await
        .expect("malformed preface should write");
    let mut server_preface = vec![0_u8; b"DROP-SECURE-V2".len()];
    stream
        .read_exact(&mut server_preface)
        .await
        .expect("server preface should write");
    stream
        .write_all(suffix)
        .await
        .expect("malformed handshake should write");
    let mut response = Vec::new();
    let result = timeout(TEST_TIMEOUT, stream.read_to_end(&mut response))
        .await
        .expect("malformed peer should be closed without hanging");
    if let Err(error) = result {
        assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::BrokenPipe
            ),
            "malformed peer returned an unexpected error: {error}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn malformed_remote_frames_are_rejected_without_panic_or_large_allocation() {
    let peer = TestPeer::new("Protocol Receiver");
    assert_malformed_secure_handshake_is_closed(&peer, &[0_u8; 13], &[]).await;
    assert_malformed_secure_handshake_is_closed(&peer, b"DROP-SECURE-V2", &1025_u32.to_be_bytes())
        .await;
    assert_malformed_secure_handshake_is_closed(&peer, b"DROP-SECURE-V2", &[1, 0xff]).await;
    assert!(peer.is_idle());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "explicit local stress pass"]
async fn stress_repeats_small_transfers_and_mixes_collisions_and_cancellation() {
    let a = TestPeer::new("Stress A");
    let b = TestPeer::new("Stress B");
    for index in 0..50 {
        let name = if index % 2 == 0 {
            "repeat.txt".to_string()
        } else {
            format!("unique-{index}.txt")
        };
        let source = a.source_dir().join(&name);
        let contents = format!("stress-{index}-{}", (index * 7919) % 104729);
        write_file(&source, contents.as_bytes());
        if index % 17 == 0 {
            write_pattern_file(&source, 2 * 96 * 1024, index as u64);
            a.events.pause_on_first_data_progress();
        }
        let run = a
            .start_send_to(&b, vec![source])
            .expect("stress transfer should start");
        let id = run.id().to_string();
        wait_incoming(&b.events, &id).await;
        b.accept(&id).expect("stress receiver should accept");
        if index % 17 == 0 {
            timeout(TEST_TIMEOUT, a.events.wait_until_paused())
                .await
                .expect("stress cancellation barrier should arrive");
            a.cancel(&id).expect("stress transfer should cancel");
            a.events.release_pause();
            wait_run(run).await;
            assert_eq!(
                wait_terminal(&a.events, &id).await.phase,
                TransferPhase::Canceled
            );
            assert_eq!(
                wait_terminal(&b.events, &id).await.phase,
                TransferPhase::Canceled
            );
        } else {
            wait_run(run).await;
            assert_eq!(
                wait_terminal(&a.events, &id).await.phase,
                TransferPhase::Completed
            );
            assert_eq!(
                wait_terminal(&b.events, &id).await.phase,
                TransferPhase::Completed
            );
        }
        assert_no_part_files(&b.destination_dir());
        wait_idle(&a, &b).await;
    }
}
