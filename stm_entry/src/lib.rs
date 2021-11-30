// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use diem_types::write_set::WriteOp;
use std::sync::RwLock;
use diem_types::transaction::ChangeSet;
use diem_types::write_set::WriteSet;
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
use serde_json;
use std::ffi::CString;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::mem;


pub struct HijackingView<S: StateView> {
    pub base_view: S,
    pub read_set: RwLock<Vec<diem_types::access_path::AccessPath>>,
}

impl <S: StateView> HijackingView<S> {
    pub fn new(base_view: S) -> Self {
        Self {
            base_view,
            read_set: RwLock::new(Vec::<diem_types::access_path::AccessPath>::new()),
        }
    }
}

impl <S: StateView> StateView for HijackingView<S> {
    fn id(&self) -> StateViewId {
        self.base_view.id()
    }

    fn get(&self, access_path: &diem_types::access_path::AccessPath) -> anyhow::Result<Option<Vec<u8>>> {
        self.read_set.write().unwrap().
            push(access_path.clone());
        self.base_view.get(access_path)
    }

    fn is_genesis(&self) -> bool {
        self.base_view.is_genesis()
    }
}


#[repr(C)]
pub struct Buffer {
    pub data: *mut u8,
    pub len: usize,
}

#[no_mangle]
pub extern "C" fn init_vm() -> *mut u8 {
    // setup the VM

    // build one transaction
    // setup read-hijacking

    let genesis = vm_genesis::generate_test_genesis(
        current_module_blobs(),
        VMPublishingOption::open(),
        None,
        false, // disable parallel
    )
    .0;

    let genesis_box = Box::new(genesis);

    let p = Box::into_raw(genesis_box) as *mut u8;
    // std::mem::forget(genesis_box);
    p
}

#[no_mangle]
pub extern "C" fn entry_vm(genesis_ptr: *mut u8, balance: u64, seq_num: u64, transfer_amount: u64) -> Buffer {
    let rng_seed: [u8; 32] = [9u8; 32];
    let mut rng = KeyGen::from_seed(rng_seed);

    let data = HashMap::<AccessPath, Vec<u8>>::new();

    let mut fake_data_store = FakeDataStore::new(data);

    let genesis = unsafe {Box::from_raw(genesis_ptr as *mut ChangeSet)};
    
    let genesis_clone = genesis.clone();

    let write_set = genesis_clone.write_set();
    std::mem::forget(genesis);

    let account_data = AccountData::new_from_seed(&mut rng, balance, seq_num);
    let account_data_2 = AccountData::new_from_seed(&mut rng, balance, seq_num);
    fake_data_store.add_write_set(&*write_set);
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

    let result = DiemVM::execute_block(vec![user_txn], &hijacking_view).unwrap();

    //println!("ReadSet {:?}", hijacking_view.read_set);
    //println!("{:?}", result);

    let read_set_vec = hijacking_view.read_set.read().unwrap();
    let write_set = result[0].write_set();

    let write_sec_vec = write_set.iter().map(|w| {
        w.clone()
    }).collect::<Vec<(AccessPath, WriteOp)>>();

    let mut lengths = vec![read_set_vec.len(), write_sec_vec.len()];
    
    let ratio = mem::size_of::<u32>() / mem::size_of::<u8>();
    let data_len = lengths.len() * ratio;
    let data_cap = lengths.capacity() * ratio;

    let data_ptr = lengths.as_mut_ptr() as *mut u8;
    mem::forget(lengths);
    let mut data = unsafe {Vec::from_raw_parts(data_ptr, data_len, data_cap)};

    // println!("metadata {}", data.len());
    

    for read_addr in read_set_vec.iter() {
        data.extend(read_addr.address.to_vec());
        data.extend(read_addr.path.clone());
    }

    for write_addr in write_sec_vec.iter() {
        data.extend(write_addr.0.address.to_vec());
        data.extend(write_addr.0.path.clone());
    }

    // encode read set and write set
    // println!("readset {}, writeset {}", read_set_vec.len(), write_sec_vec.len());

    let data_ptr = data.as_mut_ptr();
    let total_length = data.len();

    mem::forget(data);

    Buffer {
        data: data_ptr,
        len: total_length,
    }
}

#[no_mangle]
pub extern "C" fn init_entry_vm(_genesis: *const c_char, balance: u64, seq_num: u64, transfer_amount: u64) -> Buffer {
    let rng_seed: [u8; 32] = [9u8; 32];
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
    fake_data_store.add_write_set(&*write_set);
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

    let result = DiemVM::execute_block(vec![user_txn], &hijacking_view).unwrap();

    //println!("ReadSet {:?}", hijacking_view.read_set);
    //println!("{:?}", result);

    let read_set_vec = hijacking_view.read_set.read().unwrap();
    let write_set = result[0].write_set();

    let write_sec_vec = write_set.iter().map(|w| {
        w.clone()
    }).collect::<Vec<(AccessPath, WriteOp)>>();

    let mut lengths = vec![read_set_vec.len(), write_sec_vec.len()];
    
    let ratio = mem::size_of::<u32>() / mem::size_of::<u8>();
    let data_len = lengths.len() * ratio;
    let data_cap = lengths.capacity() * ratio;

    let data_ptr = lengths.as_mut_ptr() as *mut u8;
    mem::forget(lengths);
    let mut data = unsafe {Vec::from_raw_parts(data_ptr, data_len, data_cap)};

    // println!("metadata {}", data.len());
    

    for read_addr in read_set_vec.iter() {
        data.extend(read_addr.address.to_vec());
        data.extend(read_addr.path.clone());
    }

    for write_addr in write_sec_vec.iter() {
        data.extend(write_addr.0.address.to_vec());
        data.extend(write_addr.0.path.clone());
    }

    // encode read set and write set
    // println!("readset {}, writeset {}", read_set_vec.len(), write_sec_vec.len());

    let data_ptr = data.as_mut_ptr();
    let total_length = data.len();

    mem::forget(data);

    Buffer {
        data: data_ptr,
        len: total_length,
    }
}