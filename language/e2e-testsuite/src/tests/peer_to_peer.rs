// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use diem_types::{
    account_config::{ReceivedPaymentEvent, SentPaymentEvent},
    transaction::{SignedTransaction, TransactionOutput, TransactionStatus},
    vm_status::{known_locations, KeptVMStatus},
};
use language_e2e_tests::{
    account::{self, Account},
    common_transactions::peer_to_peer_txn,
    executor::FakeExecutor,
    test_with_different_versions, transaction_status_eq,
    versioning::CURRENT_RELEASE_VERSIONS,
};
use std::{convert::TryFrom, time::Instant};
use rand::Rng;

struct Zipfian {
    n: u64,
    theta: f64,
    denom: f64,
    zeta_2_theta: f64,
}

impl Zipfian {
    pub fn new(n: u64, theta: f64) -> Self {
        assert_ne!(theta, 1f64);
        let denom = Self::calculate_denom(n, theta);
        let zeta_2_theta = Self::zeta(2, theta);
        Self {
            n, theta, denom, zeta_2_theta
        }
    }

    pub fn theta(&self) -> f64 { self.theta }

    pub fn calculate_denom(n: u64, theta: f64) -> f64 {
        Self::zeta(n, theta)
    }

    pub fn zeta(n: u64, theta: f64) -> f64 {
        let mut sum = 0f64;
        for i in 1..=n {
            sum += (1.0f64/i as f64).powf(theta)
        }
        sum
    }
}

pub trait Distribution {
    fn sample(&self) -> u64;
    fn sample_usize(&self) -> usize {
        self.sample() as usize
    }
    fn describe(&self) -> String;
}

impl Distribution for Zipfian {
    fn sample(&self) -> u64 {
        let mut rng = rand::thread_rng();
        let alpha = 1f64/(1f64 - self.theta);
        let zeta_n = self.denom;
        let eta = (1f64 - (2.0f64 / self.n as f64).powf(1f64 - self.theta)) / (1f64 - self.zeta_2_theta / zeta_n);
        let u = rng.gen::<f64>();
        let uz = u * zeta_n;
        if uz < 1f64 { return 0 }
        if uz < 1f64 + 0.5f64.powf(self.theta) {return 1}
        let result = ((self.n - 1) as f64 * (eta*u -eta + 1f64).powf(alpha)).round() as u64;
        assert!(result < self.n);
        result
    }

    fn describe(&self) -> String {
        format!("Zipfian w/ theta {}", self.theta())
    }
}

#[test]
fn benchmark_zipfian() {
    let n = 1_000usize;
    let theta = 0.8;
    let zipf = Zipfian::new(n as u64, theta);
    let block_count = 10u64;
    let block_length: u64 = 100u64; // 1_000u64;
    becnhmark_with_distribution(block_count, block_length, n, Box::new(zipf));
}

fn becnhmark_with_distribution(block_count: u64, block_length: u64, n: usize, dist: Box<dyn Distribution>) {
    // Turn this on after the parallel executor is landed.
    /*
        let mut executor = FakeExecutor::from_fresh_genesis();
        let account_size = n;
        let initial_balance = 1_000_000u64;
        let initial_seq_num = 0u64;
        let accounts = executor.create_accounts(account_size, initial_balance, initial_seq_num);
        let mut seq_num_array = vec![0u64; n];

        // set up the transactions
        let transfer_amount = 1_000;
        let mut execution_time = 0f64;
        for _i in 0..block_count {
            let mut txns = vec![];
            let mut txns_info = vec![];
            for _j in 0..block_length {
                let sender_id = dist.sample_usize();
                let receiver_id = dist.sample_usize();
                let sender = &accounts[sender_id];
                let receiver = &accounts[receiver_id];
                let seq_num = seq_num_array[sender_id];
                seq_num_array[sender_id] += 1;
                let txn = peer_to_peer_txn(sender, receiver, seq_num, transfer_amount);
                txns.push(txn);
                txns_info.push(TxnInfo::new(sender, receiver, transfer_amount));
            }

            // execute transaction
            let now = Instant::now();
            let output = executor.execute_block(txns).unwrap();
            println!("txn output {:?}", output.clone());
            execution_time += now.elapsed().as_secs_f64();
            for txn_output in &output {
                assert_eq!(
                    txn_output.status(),
                    &TransactionStatus::Keep(KeptVMStatus::Executed)
                );
            }
            check_and_apply_transfer_output(&mut executor, &txns_info, &output);
        }
        let throughput = (block_count * block_length) as f64 / execution_time;
        println!("Zipfian Benchmark:\nThroughput: {}, Execution Time: {}, dist: {}", throughput, execution_time, dist.describe());
    */
}


#[test]
fn single_peer_to_peer_with_event() {
    ::diem_logger::Logger::init_for_testing();
    test_with_different_versions! {CURRENT_RELEASE_VERSIONS, |test_env| {
        let mut executor = test_env.executor;
        // create and publish a sender with 1_000_000 coins and a receiver with 100_000 coins
        let sender = executor.create_raw_account_data(1_000_000, 10);
        let receiver = executor.create_raw_account_data(100_000, 10);
        executor.add_account_data(&sender);
        executor.add_account_data(&receiver);

        let transfer_amount = 1_000;
        let txn = peer_to_peer_txn(sender.account(), receiver.account(), 10, transfer_amount);

        // execute transaction
        let output = executor.execute_transaction(txn);
        assert_eq!(
            output.status(),
            &TransactionStatus::Keep(KeptVMStatus::Executed)
        );

        executor.apply_write_set(output.write_set());

        // check that numbers in stored DB are correct
        let sender_balance = 1_000_000 - transfer_amount;
        let receiver_balance = 100_000 + transfer_amount;
        let updated_sender = executor
            .read_account_resource(sender.account())
            .expect("sender must exist");
        let updated_sender_balance = executor
            .read_balance_resource(sender.account(), account::xus_currency_code())
            .expect("sender balance must exist");
        let updated_receiver = executor
            .read_account_resource(receiver.account())
            .expect("receiver must exist");
        let updated_receiver_balance = executor
            .read_balance_resource(receiver.account(), account::xus_currency_code())
            .expect("receiver balance must exist");
        assert_eq!(receiver_balance, updated_receiver_balance.coin());
        assert_eq!(sender_balance, updated_sender_balance.coin());
        assert_eq!(11, updated_sender.sequence_number());
        assert_eq!(0, updated_sender.received_events().count(),);
        assert_eq!(1, updated_sender.sent_events().count());
        assert_eq!(1, updated_receiver.received_events().count());
        assert_eq!(0, updated_receiver.sent_events().count());

        let rec_ev_path = receiver.received_events_key().to_vec();
        let sent_ev_path = sender.sent_events_key().to_vec();
        for event in output.events() {
            assert!(
                rec_ev_path.as_slice() == event.key().as_bytes()
                    || sent_ev_path.as_slice() == event.key().as_bytes()
            );
        }
    }
    }
}

// TODO test no longer simple as the legacy version takes an &signer but all
// new scripts take an owned signer
// #[test]
// fn single_peer_to_peer_with_padding() {
//     ::diem_logger::Logger::init_for_testing();
//     // create a FakeExecutor with a genesis from file
//     let mut executor =
//         FakeExecutor::from_genesis_with_options(VMPublishingOption::custom_scripts());
//     executor.set_golden_file(current_function_name!());

//     // create and publish a sender with 1_000_000 coins and a receiver with 100_000 coins
//     let sender = executor.create_raw_account_data(1_000_000, 10);
//     let receiver = executor.create_raw_account_data(100_000, 10);
//     executor.add_account_data(&sender);
//     executor.add_account_data(&receiver);

//     let transfer_amount = 1_000;
//     let padded_script = {
//         let mut script_mut = CompiledScript::deserialize(
//             &LegacyStdlibScript::PeerToPeerWithMetadata
//                 .compiled_bytes()
//                 .into_vec(),
//         )
//         .unwrap()
//         .into_inner();
//         script_mut
//             .code
//             .code
//             .extend(std::iter::repeat(Bytecode::Ret).take(1000));
//         let mut script_bytes = vec![];
//         script_mut
//             .freeze()
//             .unwrap()
//             .serialize(&mut script_bytes)
//             .unwrap();

//         Script::new(
//             script_bytes,
//             vec![account_config::xus_tag()],
//             vec![
//                 TransactionArgument::Address(*receiver.address()),
//                 TransactionArgument::U64(transfer_amount),
//                 TransactionArgument::U8Vector(vec![]),
//                 TransactionArgument::U8Vector(vec![]),
//             ],
//         )
//     };

//     let txn = sender
//         .account()
//         .transaction()
//         .script(padded_script)
//         .sequence_number(10)
//         .sign();
//     let unpadded_txn = peer_to_peer_txn(sender.account(), receiver.account(), 10, transfer_amount);
//     assert!(txn.raw_txn_bytes_len() > unpadded_txn.raw_txn_bytes_len());
//     // execute transaction
//     let output = executor.execute_transaction(txn);
//     assert_eq!(
//         output.status(),
//         &TransactionStatus::Keep(KeptVMStatus::Executed)
//     );

//     executor.apply_write_set(output.write_set());

//     // check that numbers in stored DB are correct
//     let sender_balance = 1_000_000 - transfer_amount;
//     let receiver_balance = 100_000 + transfer_amount;
//     let updated_sender = executor
//         .read_account_resource(sender.account())
//         .expect("sender must exist");
//     let updated_sender_balance = executor
//         .read_balance_resource(sender.account(), account::xus_currency_code())
//         .expect("sender balance must exist");
//     let updated_receiver_balance = executor
//         .read_balance_resource(receiver.account(), account::xus_currency_code())
//         .expect("receiver balance must exist");
//     assert_eq!(receiver_balance, updated_receiver_balance.coin());
//     assert_eq!(sender_balance, updated_sender_balance.coin());
//     assert_eq!(11, updated_sender.sequence_number());
// }

#[test]
fn few_peer_to_peer_with_event() {
    test_with_different_versions! {CURRENT_RELEASE_VERSIONS, |test_env| {
        let mut executor = test_env.executor;

        // create and publish a sender with 3_000_000 coins and a receiver with 3_000_000 coins
        let sender = executor.create_raw_account_data(3_000_000, 10);
        let receiver = executor.create_raw_account_data(3_000_000, 10);
        executor.add_account_data(&sender);
        executor.add_account_data(&receiver);

        let transfer_amount = 1_000;

        // execute transaction
        let txns: Vec<SignedTransaction> = vec![
            peer_to_peer_txn(sender.account(), receiver.account(), 10, transfer_amount),
            peer_to_peer_txn(sender.account(), receiver.account(), 11, transfer_amount),
            peer_to_peer_txn(sender.account(), receiver.account(), 12, transfer_amount),
            peer_to_peer_txn(sender.account(), receiver.account(), 13, transfer_amount),
        ];
        let output = executor.execute_block(txns).unwrap();
        for (idx, txn_output) in output.iter().enumerate() {
            assert_eq!(
                txn_output.status(),
                &TransactionStatus::Keep(KeptVMStatus::Executed)
            );

            // check events
            for event in txn_output.events() {
                if let Ok(payload) = SentPaymentEvent::try_from(event) {
                    assert_eq!(transfer_amount, payload.amount());
                    assert_eq!(receiver.address(), &payload.receiver());
                } else if let Ok(payload) = ReceivedPaymentEvent::try_from(event) {
                    assert_eq!(transfer_amount, payload.amount());
                    assert_eq!(sender.address(), &payload.sender());
                } else {
                    panic!("Unexpected Event Type")
                }
            }

            let original_sender_balance = executor
                .read_balance_resource(sender.account(), account::xus_currency_code())
                .expect("sender balance must exist");
            let original_receiver_balance = executor
                .read_balance_resource(receiver.account(), account::xus_currency_code())
                .expect("receiver balcne must exist");
            executor.apply_write_set(txn_output.write_set());

            // check that numbers in stored DB are correct
            let sender_balance = original_sender_balance.coin() - transfer_amount;
            let receiver_balance = original_receiver_balance.coin() + transfer_amount;
            let updated_sender = executor
                .read_account_resource(sender.account())
                .expect("sender must exist");
            let updated_sender_balance = executor
                .read_balance_resource(sender.account(), account::xus_currency_code())
                .expect("sender balance must exist");
            let updated_receiver = executor
                .read_account_resource(receiver.account())
                .expect("receiver must exist");
            let updated_receiver_balance = executor
                .read_balance_resource(receiver.account(), account::xus_currency_code())
                .expect("receiver balance must exist");
            assert_eq!(receiver_balance, updated_receiver_balance.coin());
            assert_eq!(sender_balance, updated_sender_balance.coin());
            assert_eq!(11 + idx as u64, updated_sender.sequence_number());
            assert_eq!(0, updated_sender.received_events().count());
            assert_eq!(idx as u64 + 1, updated_sender.sent_events().count());
            assert_eq!(idx as u64 + 1, updated_receiver.received_events().count());
            assert_eq!(0, updated_receiver.sent_events().count());
        }
    }
    }
}

/// Test that a zero-amount transaction fails, per policy.
#[test]
fn zero_amount_peer_to_peer() {
    test_with_different_versions! {CURRENT_RELEASE_VERSIONS, |test_env| {
        let mut executor = test_env.executor;
        let sequence_number = 10;
        let sender = executor.create_raw_account_data(1_000_000, sequence_number);
        let receiver = executor.create_raw_account_data(100_000, sequence_number);
        executor.add_account_data(&sender);
        executor.add_account_data(&receiver);

        let transfer_amount = 0;
        let txn = peer_to_peer_txn(
            sender.account(),
            receiver.account(),
            sequence_number,
            transfer_amount,
        );

        let output = &executor.execute_transaction(txn);
        // Error code 7 means that the transaction was a zero-amount one.
        assert!(transaction_status_eq(
            &output.status(),
            &TransactionStatus::Keep(KeptVMStatus::MoveAbort(
                known_locations::account_module_abort(),
                519
            )),
        ));
    }
    }
}

// Holder for transaction data; arguments to transactions.
struct TxnInfo {
    pub sender: Account,
    pub receiver: Account,
    pub transfer_amount: u64,
}

impl TxnInfo {
    fn new(sender: &Account, receiver: &Account, transfer_amount: u64) -> Self {
        TxnInfo {
            sender: sender.clone(),
            receiver: receiver.clone(),
            transfer_amount,
        }
    }
}

// Create a cyclic transfer around a slice of Accounts.
// Each Account makes a transfer for the same amount to the next DiemAccount.
fn create_cyclic_transfers(
    executor: &FakeExecutor,
    accounts: &[Account],
    transfer_amount: u64,
) -> (Vec<TxnInfo>, Vec<SignedTransaction>) {
    let mut txns: Vec<SignedTransaction> = Vec::new();
    let mut txns_info: Vec<TxnInfo> = Vec::new();
    // loop through all transactions and let each transfer the same amount to the next one
    let count = accounts.len();
    for i in 0..count {
        let sender = &accounts[i];
        let sender_resource = executor
            .read_account_resource(&sender)
            .expect("sender must exist");
        let seq_num = sender_resource.sequence_number();
        let receiver = &accounts[(i + 1) % count];

        let txn = peer_to_peer_txn(sender, receiver, seq_num, transfer_amount);
        txns.push(txn);
        txns_info.push(TxnInfo::new(sender, receiver, transfer_amount));
    }
    (txns_info, txns)
}

// Create a one to many transfer around a slice of Accounts.
// The first account is the payer and all others are receivers.
fn create_one_to_many_transfers(
    executor: &FakeExecutor,
    accounts: &[Account],
    transfer_amount: u64,
) -> (Vec<TxnInfo>, Vec<SignedTransaction>) {
    let mut txns: Vec<SignedTransaction> = Vec::new();
    let mut txns_info: Vec<TxnInfo> = Vec::new();
    // grab account 0 as a sender
    let sender = &accounts[0];
    let sender_resource = executor
        .read_account_resource(&sender)
        .expect("sender must exist");
    let seq_num = sender_resource.sequence_number();
    // loop through all transactions and let each transfer the same amount to the next one
    let count = accounts.len();
    for (i, receiver) in accounts.iter().enumerate().take(count).skip(1) {
        // let receiver = &accounts[i];

        let txn = peer_to_peer_txn(sender, receiver, seq_num + i as u64 - 1, transfer_amount);
        txns.push(txn);
        txns_info.push(TxnInfo::new(sender, receiver, transfer_amount));
    }
    (txns_info, txns)
}

// Create a many to one transfer around a slice of Accounts.
// The first account is the receiver and all others are payers.
fn create_many_to_one_transfers(
    executor: &FakeExecutor,
    accounts: &[Account],
    transfer_amount: u64,
) -> (Vec<TxnInfo>, Vec<SignedTransaction>) {
    let mut txns: Vec<SignedTransaction> = Vec::new();
    let mut txns_info: Vec<TxnInfo> = Vec::new();
    // grab account 0 as a sender
    let receiver = &accounts[0];
    // loop through all transactions and let each transfer the same amount to the next one
    let count = accounts.len();
    for sender in accounts.iter().take(count).skip(1) {
        //let sender = &accounts[i];
        let sender_resource = executor
            .read_account_resource(sender)
            .expect("sender must exist");
        let seq_num = sender_resource.sequence_number();

        let txn = peer_to_peer_txn(sender, receiver, seq_num, transfer_amount);
        txns.push(txn);
        txns_info.push(TxnInfo::new(sender, receiver, transfer_amount));
    }
    (txns_info, txns)
}

// Verify a transfer output.
// Checks that sender and receiver in a peer to peer transaction are in proper
// state after a successful transfer.
// The transaction arguments are provided in txn_args.
// Apply the WriteSet to the data store.
fn check_and_apply_transfer_output(
    executor: &mut FakeExecutor,
    txn_args: &[TxnInfo],
    output: &[TransactionOutput],
) {
    let count = output.len();
    for i in 0..count {
        let txn_info = &txn_args[i];
        let sender = &txn_info.sender;
        let receiver = &txn_info.receiver;
        let transfer_amount = txn_info.transfer_amount;
        let sender_resource = executor
            .read_account_resource(&sender)
            .expect("sender must exist");
        let sender_balance = executor
            .read_balance_resource(&sender, account::xus_currency_code())
            .expect("sender balance must exist");
        let sender_initial_balance = sender_balance.coin();
        let sender_seq_num = sender_resource.sequence_number();
        let receiver_initial_balance = executor
            .read_balance_resource(&receiver, account::xus_currency_code())
            .expect("receiver balance must exist")
            .coin();

        // apply single transaction to DB
        let txn_output = &output[i];
        executor.apply_write_set(txn_output.write_set());

        // check that numbers stored in DB are correct
        let sender_balance = sender_initial_balance - transfer_amount;
        let receiver_balance = receiver_initial_balance + transfer_amount;
        let updated_sender = executor
            .read_account_resource(&sender)
            .expect("sender must exist");
        let updated_sender_balance = executor
            .read_balance_resource(&sender, account::xus_currency_code())
            .expect("sender balance must exist");
        let updated_receiver_balance = executor
            .read_balance_resource(&receiver, account::xus_currency_code())
            .expect("receiver balance must exist");
        assert_eq!(receiver_balance, updated_receiver_balance.coin());
        assert_eq!(sender_balance, updated_sender_balance.coin());
        assert_eq!(sender_seq_num + 1, updated_sender.sequence_number());
    }
}

// simple utility to print all account to visually inspect account data
fn print_accounts(executor: &FakeExecutor, accounts: &[Account]) {
    for account in accounts {
        let account_resource = executor
            .read_account_resource(&account)
            .expect("sender must exist");
        println!("{:?}", account_resource);
    }
}

#[test]
fn cycle_peer_to_peer() {
    test_with_different_versions! {CURRENT_RELEASE_VERSIONS, |test_env| {
        let mut executor = test_env.executor;
        let account_size = 100usize;
        let initial_balance = 2_000_000u64;
        let initial_seq_num = 10u64;
        let accounts = executor.create_accounts(account_size, initial_balance, initial_seq_num);

        // set up the transactions
        let transfer_amount = 1_000;
        let (txns_info, txns) = create_cyclic_transfers(&executor, &accounts, transfer_amount);

        // execute transaction
        let mut execution_time = 0u128;
        let now = Instant::now();
        let output = executor.execute_block(txns).unwrap();
        execution_time += now.elapsed().as_nanos();
        println!("EXECUTION TIME: {}", execution_time);
        for txn_output in &output {
            assert_eq!(
                txn_output.status(),
                &TransactionStatus::Keep(KeptVMStatus::Executed)
            );
        }
        assert_eq!(accounts.len(), output.len());

        check_and_apply_transfer_output(&mut executor, &txns_info, &output);
        print_accounts(&executor, &accounts);
    }
    }
}

#[test]
fn cycle_peer_to_peer_multi_block() {
    test_with_different_versions! {CURRENT_RELEASE_VERSIONS, |test_env| {
        let mut executor = test_env.executor;
        let account_size = 100usize;
        let initial_balance = 1_000_000u64;
        let initial_seq_num = 10u64;
        let accounts = executor.create_accounts(account_size, initial_balance, initial_seq_num);

        // set up the transactions
        let transfer_amount = 1_000;
        let block_count = 5u64;
        let cycle = account_size / (block_count as usize);
        let mut range_left = 0usize;
        let mut execution_time = 0u128;
        for _i in 0..block_count {
            range_left = if range_left + cycle >= account_size {
                account_size - cycle
            } else {
                range_left
            };
            let (txns_info, txns) = create_cyclic_transfers(
                &executor,
                &accounts[range_left..range_left + cycle],
                transfer_amount,
            );

            // execute transaction
            let now = Instant::now();
            let output = executor.execute_block(txns).unwrap();
            execution_time += now.elapsed().as_nanos();
            for txn_output in &output {
                assert_eq!(
                    txn_output.status(),
                    &TransactionStatus::Keep(KeptVMStatus::Executed)
                );
            }
            assert_eq!(cycle, output.len());
            check_and_apply_transfer_output(&mut executor, &txns_info, &output);
            range_left = (range_left + cycle) % account_size;
        }
        println!("EXECUTION TIME: {}", execution_time);
        print_accounts(&executor, &accounts);
    }
    }
}

#[test]
fn one_to_many_peer_to_peer() {
    test_with_different_versions! {CURRENT_RELEASE_VERSIONS, |test_env| {
        let mut executor = test_env.executor;
        let account_size = 100usize;
        let initial_balance = 100_000_000u64;
        let initial_seq_num = 10u64;
        let accounts = executor.create_accounts(account_size, initial_balance, initial_seq_num);

        // set up the transactions
        let transfer_amount = 1_000;
        let block_count = 2u64;
        let cycle = account_size / (block_count as usize);
        let mut range_left = 0usize;
        let mut execution_time = 0u128;
        for _i in 0..block_count {
            range_left = if range_left + cycle >= account_size {
                account_size - cycle
            } else {
                range_left
            };
            let (txns_info, txns) = create_one_to_many_transfers(
                &executor,
                &accounts[range_left..range_left + cycle],
                transfer_amount,
            );

            // execute transaction
            let now = Instant::now();
            let output = executor.execute_block(txns).unwrap();
            execution_time += now.elapsed().as_nanos();
            for txn_output in &output {
                assert_eq!(
                    txn_output.status(),
                    &TransactionStatus::Keep(KeptVMStatus::Executed)
                );
            }
            assert_eq!(cycle - 1, output.len());
            check_and_apply_transfer_output(&mut executor, &txns_info, &output);
            range_left = (range_left + cycle) % account_size;
        }
        println!("EXECUTION TIME: {}", execution_time);
        print_accounts(&executor, &accounts);
    }
    }
}

#[test]
fn many_to_one_peer_to_peer() {
    test_with_different_versions! {CURRENT_RELEASE_VERSIONS, |test_env| {
        let mut executor = test_env.executor;
        let account_size = 100usize;
        let initial_balance = 1_000_000u64;
        let initial_seq_num = 10u64;
        let accounts = executor.create_accounts(account_size, initial_balance, initial_seq_num);

        // set up the transactions
        let transfer_amount = 1_000;
        let block_count = 2u64;
        let cycle = account_size / (block_count as usize);
        let mut range_left = 0usize;
        let mut execution_time = 0u128;
        for _i in 0..block_count {
            range_left = if range_left + cycle >= account_size {
                account_size - cycle
            } else {
                range_left
            };
            let (txns_info, txns) = create_many_to_one_transfers(
                &executor,
                &accounts[range_left..range_left + cycle],
                transfer_amount,
            );

            // execute transaction
            let now = Instant::now();
            let output = executor.execute_block(txns).unwrap();
            execution_time += now.elapsed().as_nanos();
            for txn_output in &output {
                assert_eq!(
                    txn_output.status(),
                    &TransactionStatus::Keep(KeptVMStatus::Executed)
                );
            }
            assert_eq!(cycle - 1, output.len());
            check_and_apply_transfer_output(&mut executor, &txns_info, &output);
            range_left = (range_left + cycle) % account_size;
        }
        println!("EXECUTION TIME: {}", execution_time);
        print_accounts(&executor, &accounts);
    }
    }
}
