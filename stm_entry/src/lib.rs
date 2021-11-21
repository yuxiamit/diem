// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use diem_vm::{DiemVM, VMExecutor};
use diem_state_view::{StateView, StateViewId};
use diem_types::access_path::AccessPath;
use diem_types::transaction::Transaction;
use language_e2e_tests::account::Account;
use language_e2e_tests::common_transactions::peer_to_peer_txn;
use language_e2e_tests::data_store::FakeDataStore;
use anyhow;

pub struct HijackingView<S: StateView> {
    base_view: S,
}

impl <S: StateView> HijackingView<S> {
    pub fn new(base_view: S) -> Self {
        Self {
            base_view
        }
    }
}

impl <S: StateView> StateView for HijackingView<S> {
    fn id(&self) -> StateViewId {
        self.base_view.id()
    }

    fn get(&self, access_path: &diem_types::access_path::AccessPath) -> anyhow::Result<Option<Vec<u8>>> {
        self.base_view.get(access_path)
    }

    fn is_genesis(&self) -> bool {
        self.base_view.is_genesis()
    }
}

#[no_mangle]
pub extern "C" fn entry_vm(idx: usize, from: u64, to: u64, seq_num: u64, transfer_amount: u64) {
    // build one transaction
    // setup read-hijacking

    // overhead of gen-keypair, TODO: use from to to
    let txn = peer_to_peer_txn(
        &Account::new(),
        &Account::new(),
        seq_num,
        transfer_amount,
    );

    let data = HashMap::<AccessPath, Vec<u8>>::new();

    let fake_data_store = FakeDataStore::new(data);

    let hijacking_view = HijackingView::new(fake_data_store);

    let user_txn = Transaction::UserTransaction(txn);

    let result = DiemVM::execute_block(vec![user_txn], &hijacking_view);

    println!("{:?}", result);
}