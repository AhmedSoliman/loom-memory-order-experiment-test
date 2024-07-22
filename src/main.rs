use std::sync::Arc;

#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[cfg(loom)]
use loom::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering};

struct Logger {
    last_offset: AtomicUsize,
    sealed: AtomicBool,
    next_offset: AtomicUsize,
    release_pointer: AtomicUsize,
}

impl Logger {
    fn new() -> Self {
        Logger {
            last_offset: AtomicUsize::new(0),
            sealed: AtomicBool::new(false),
            next_offset: AtomicUsize::new(1),
            release_pointer: AtomicUsize::new(0),
        }
    }

    pub fn append(&self) -> Result<usize, &'static str> {
        // Check if sealed
        if self.sealed.load(Ordering::Relaxed) {
            return Err("Logger is sealed");
        }

        let offset = self.next_offset.fetch_add(1, Ordering::Relaxed);
        #[cfg(loom)]
        loom::thread::yield_now();
        let release_pointer = self
            .last_offset
            .fetch_max(offset, Ordering::AcqRel)
            .max(offset);

        self.release_pointer
            .store(release_pointer, Ordering::Relaxed);

        // After incrementing, check again if sealed
        if self.sealed.load(Ordering::Relaxed) {
            println!(
                "Sealed when my_offset={}, last_offset={}",
                offset,
                self.last_offset.load(Ordering::Relaxed)
            );
            return Err("Logger is sealed");
        }

        Ok(offset)
    }

    pub fn seal(&self) {
        if self.sealed.load(Ordering::Relaxed) {
            return;
        }
        self.sealed.store(true, Ordering::Relaxed);
    }

    pub fn find_tail(&self) -> (bool, usize) {
        let last_offset = self.last_offset.fetch_add(0, Ordering::Release) + 1;
        let sealed = self.sealed.load(Ordering::Relaxed);

        (sealed, last_offset)
    }
}

fn main() {
    let logger = Arc::new(Logger::new());
    println!("tail is: {:?}", logger.find_tail());
    println!("appended {}", logger.append().unwrap());
    println!("appended {}", logger.append().unwrap());
    println!("tail is: {:?}", logger.find_tail());
    println!("sealing....");
    logger.seal();
    println!("tail is: {:?}", logger.find_tail());
    println!("append err: {:?}", logger.append());
    println!("tail is: {:?}", logger.find_tail());
}

#[cfg(loom)]
#[test]
fn test_single_tail_correctness() {
    loom::model(|| {
        let logger = Arc::new(Logger::new());

        // Simulate append and seal operations
        let logger_clone = Arc::clone(&logger);
        let t_appends = loom::thread::spawn(move || logger_clone.append().unwrap_or(0));

        // reads
        let logger_clone = Arc::clone(&logger);
        let t_reads = loom::thread::spawn(move || loop {
            loom::thread::yield_now();
            let (sealed, offset) = logger_clone.find_tail();
            if sealed {
                return offset;
            }
        });

        // Simulate sealing
        let logger_clone = Arc::clone(&logger);
        let t_seal = loom::thread::spawn(move || {
            loom::thread::yield_now();
            logger_clone.seal();
        });

        let appended = t_appends.join().unwrap();
        t_seal.join().unwrap();
        let first_observed_sealed = t_reads.join().unwrap();

        // Get the tail
        let tail = logger.find_tail();
        assert!(tail.1 >= appended);
        println!(
            "Tail: sealed={}, tail={}, last_acked_offset={}, first_tail_seal_observed={}, release_pointer={}",
            tail.0,
            tail.1,
            appended,
            first_observed_sealed,
            logger.release_pointer.load(Ordering::Relaxed),
        );

        // observed sealed must higher or equal to last successful append
        assert!(first_observed_sealed > appended);
        assert!(tail.1 >= first_observed_sealed);
    });
}
