// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use diem_framework_releases::current_module_blobs;
use diem_keygen::KeyGen;
use diem_types::on_chain_config::VMPublishingOption;
use diem_vm::{DiemVM, VMExecutor};
use diem_state_view::{StateView, StateViewId};
use diem_types::access_path::AccessPath;
use diem_types::transaction::Transaction;
use language_e2e_tests::account::{Account, AccountData};
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
pub extern "C" fn entry_vm(_idx: usize, _from: u64, _to: u64, seq_num: u64, transfer_amount: u64) {
    // build one transaction
    // setup read-hijacking

    let rng_seed: [u8; 32] = [9u8; 32];

    let balance = 100;
    let mut rng = KeyGen::from_seed(rng_seed);

    
    let data = HashMap::<AccessPath, Vec<u8>>::new();

    let mut fake_data_store = FakeDataStore::new(data);
    
    let genesis = vm_genesis::generate_test_genesis(
        current_module_blobs(),
        VMPublishingOption::open(),
        None,
        false, // disable parallel
    )
    .0;

    let write_set = genesis.write_set();

    let account_data = AccountData::new_from_seed(&mut rng, balance, seq_num);
    let account_data_2 = AccountData::new_from_seed(&mut rng, balance, seq_num);
    fake_data_store.add_write_set(write_set);
    fake_data_store.add_account_data(&account_data);
    fake_data_store.add_account_data(&account_data_2);

    let hijacking_view = HijackingView::new(fake_data_store);

    // overhead of gen-keypair, TODO: use from to to
    let txn = peer_to_peer_txn(
        &account_data.account(),
        &account_data_2.account(),
        seq_num,
        transfer_amount,
    );

    let user_txn = Transaction::UserTransaction(txn);

    let result = DiemVM::execute_block(vec![user_txn], &hijacking_view);

    println!("{:?}", result);
}