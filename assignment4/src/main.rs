//concurrency assignment4
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use rand::Rng;

const TERMINATION_SIGNAL: i32 = -1;

fn main() {
    const ITEM_COUNT: usize = 20;
    let num_consumers = 3;

    let (tx, rx) = mpsc::channel();
    let rx = Arc::new(Mutex::new(rx));

    let mut handles = vec![];

    // Producers
    for id in 0..2 {
        let tx_clone = tx.clone();
        handles.push(thread::spawn(move || {
            producer(id, tx_clone, ITEM_COUNT / 2);
        }));
    }

    // Consumers
    for id in 0..num_consumers {
        let rx_clone = Arc::clone(&rx);
        handles.push(thread::spawn(move || {
            consumer(id, rx_clone);
        }));
    }

    // Wait for producers to finish
    thread::sleep(Duration::from_secs(2));

    // Send termination signals
    for _ in 0..num_consumers {
        tx.send(TERMINATION_SIGNAL).unwrap();
    }

    // Join all threads
    for handle in handles {
        handle.join().unwrap();
    }

    println!("All items have been produced and consumed!");
}

// Producer
fn producer(id: usize, tx: mpsc::Sender<i32>, item_count: usize) {
    let mut rng = rand::thread_rng();

    for _ in 0..item_count {
        let num = rng.gen_range(1..100);
        println!("Producer {} produced {}", id, num);
        tx.send(num).unwrap();
        thread::sleep(Duration::from_millis(100));
    }

    println!("Producer {} finished", id);
}

// Consumer
fn consumer(id: usize, rx: Arc<Mutex<mpsc::Receiver<i32>>>) {
    loop {
        let value = rx.lock().unwrap().recv().unwrap();

        if value == TERMINATION_SIGNAL {
            println!("Consumer {} terminating", id);
            break;
        }

        println!("Consumer {} processing {}", id, value);
        thread::sleep(Duration::from_millis(200));
    }
}