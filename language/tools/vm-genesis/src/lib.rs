// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

mod genesis_context;
pub mod genesis_gas_schedule;

use crate::{genesis_context::GenesisStateView, genesis_gas_schedule::INITIAL_GAS_SCHEDULE};
use diem_crypto::{
    ed25519::{Ed25519PrivateKey, Ed25519PublicKey},
    PrivateKey, Uniform,
};
use diem_framework_releases::{
    current_module_blobs, legacy::transaction_scripts::LegacyStdlibScript,
};
use diem_transaction_builder::stdlib as transaction_builder;
use diem_types::{
    account_address,
    account_config::{
        self,
        events::{CreateAccountEvent, NewEpochEvent},
    },
    chain_id::{ChainId, NamedChain},
    contract_event::ContractEvent,
    on_chain_config::{VMPublishingOption, DIEM_MAX_KNOWN_VERSION},
    transaction::{
        authenticator::AuthenticationKey, ChangeSet, ScriptFunction, Transaction, WriteSetPayload,
    },
};
use diem_vm::{convert_changeset_and_events, data_cache::StateViewCache};
use move_binary_format::CompiledModule;
use move_bytecode_utils::Modules;
use move_core_types::{
    account_address::AccountAddress,
    identifier::Identifier,
    language_storage::{ModuleId, StructTag, TypeTag},
    value::{serialize_values, MoveValue},
};
use move_vm_runtime::{
    logging::{LogContext, NoContextLog},
    move_vm::MoveVM,
    session::Session,
};
use move_vm_types::gas_schedule::GasStatus;
use once_cell::sync::Lazy;
use rand::prelude::*;
use transaction_builder::encode_create_designated_dealer_script_function;

// The seed is arbitrarily picked to produce a consistent key. XXX make this more formal?
const GENESIS_SEED: [u8; 32] = [42; 32];

const GENESIS_MODULE_NAME: &str = "Genesis";
const DIEM_VERSION_MODULE_NAME: &str = "DiemVersion";

pub static GENESIS_KEYPAIR: Lazy<(Ed25519PrivateKey, Ed25519PublicKey)> = Lazy::new(|| {
    let mut rng = StdRng::from_seed(GENESIS_SEED);
    let private_key = Ed25519PrivateKey::generate(&mut rng);
    let public_key = private_key.public_key();
    (private_key, public_key)
});

const ZERO_AUTH_KEY: [u8; 32] = [0; 32];

pub type Name = Vec<u8>;
// Defines a validator owner and maps that to an operator
pub type OperatorAssignment = (Option<Ed25519PublicKey>, Name, ScriptFunction);

// Defines a validator operator and maps that to a validator (config)
pub type OperatorRegistration = (Ed25519PublicKey, Name, ScriptFunction);

pub fn encode_genesis_transaction(
    diem_root_key: Ed25519PublicKey,
    treasury_compliance_key: Ed25519PublicKey,
    operator_assignments: &[OperatorAssignment],
    operator_registrations: &[OperatorRegistration],
    vm_publishing_option: Option<VMPublishingOption>,
    chain_id: ChainId,
) -> Transaction {
    Transaction::GenesisTransaction(WriteSetPayload::Direct(encode_genesis_change_set(
        &diem_root_key,
        &treasury_compliance_key,
        operator_assignments,
        operator_registrations,
        current_module_blobs(), // Must use compiled stdlib,
        vm_publishing_option
            .unwrap_or_else(|| VMPublishingOption::locked(LegacyStdlibScript::allowlist())),
        chain_id,
    )))
}

pub fn encode_genesis_change_set(
    diem_root_key: &Ed25519PublicKey,
    treasury_compliance_key: &Ed25519PublicKey,
    operator_assignments: &[OperatorAssignment],
    operator_registrations: &[OperatorRegistration],
    stdlib_module_bytes: &[Vec<u8>],
    vm_publishing_option: VMPublishingOption,
    chain_id: ChainId,
) -> ChangeSet {
    let mut stdlib_modules = Vec::new();
    // create a data view for move_vm
    let mut state_view = GenesisStateView::new();
    for module_bytes in stdlib_module_bytes {
        let module = CompiledModule::deserialize(module_bytes).unwrap();
        state_view.add_module(&module.self_id(), &module_bytes);
        stdlib_modules.push(module)
    }
    let data_cache = StateViewCache::new(&state_view);

    let move_vm = MoveVM::new();
    let mut session = move_vm.new_session(&data_cache);
    let log_context = NoContextLog::new();

    let xdx_ty = TypeTag::Struct(StructTag {
        address: *account_config::XDX_MODULE.address(),
        module: account_config::XDX_MODULE.name().to_owned(),
        name: account_config::XDX_IDENTIFIER.to_owned(),
        type_params: vec![],
    });

    create_and_initialize_main_accounts(
        &mut session,
        &log_context,
        &diem_root_key,
        &treasury_compliance_key,
        vm_publishing_option,
        &xdx_ty,
        chain_id,
    );
    // generate the genesis WriteSet
    create_and_initialize_owners_operators(
        &mut session,
        &log_context,
        &operator_assignments,
        &operator_registrations,
    );
    reconfigure(&mut session, &log_context);

    if [NamedChain::TESTNET, NamedChain::DEVNET, NamedChain::TESTING]
        .iter()
        .any(|test_chain_id| test_chain_id.id() == chain_id.id())
    {
        create_and_initialize_testnet_minting(&mut session, &log_context, &treasury_compliance_key);
    }

    let (mut changeset1, mut events1) = session.finish().unwrap();

    let state_view = GenesisStateView::new();
    let data_cache = StateViewCache::new(&state_view);
    let mut session = move_vm.new_session(&data_cache);
    publish_stdlib(
        &mut session,
        &log_context,
        Modules::new(stdlib_modules.iter()),
    );
    let (changeset2, events2) = session.finish().unwrap();

    changeset1.squash(changeset2).unwrap();
    events1.extend(events2);

    let (write_set, events) = convert_changeset_and_events(changeset1, events1).unwrap();

    assert!(!write_set.iter().any(|(_, op)| op.is_deletion()));
    verify_genesis_write_set(&events);
    ChangeSet::new(write_set, events)
}

fn exec_function(
    session: &mut Session<StateViewCache>,
    log_context: &impl LogContext,
    module_name: &str,
    function_name: &str,
    ty_args: Vec<TypeTag>,
    args: Vec<Vec<u8>>,
) {
    session
        .execute_function(
            &ModuleId::new(
                account_config::CORE_CODE_ADDRESS,
                Identifier::new(module_name).unwrap(),
            ),
            &Identifier::new(function_name).unwrap(),
            ty_args,
            args,
            &mut GasStatus::new_unmetered(),
            log_context,
        )
        .unwrap_or_else(|e| {
            panic!(
                "Error calling {}.{}: {}",
                module_name,
                function_name,
                e.into_vm_status()
            )
        });
}

fn exec_script_function(
    session: &mut Session<StateViewCache>,
    log_context: &impl LogContext,
    sender: AccountAddress,
    script_function: &ScriptFunction,
) {
    session
        .execute_script_function(
            script_function.module(),
            script_function.function(),
            script_function.ty_args().to_vec(),
            script_function.args().to_vec(),
            vec![sender],
            &mut GasStatus::new_unmetered(),
            log_context,
        )
        .unwrap()
}

/// Create and initialize Association and Core Code accounts.
fn create_and_initialize_main_accounts(
    session: &mut Session<StateViewCache>,
    log_context: &impl LogContext,
    diem_root_key: &Ed25519PublicKey,
    treasury_compliance_key: &Ed25519PublicKey,
    publishing_option: VMPublishingOption,
    xdx_ty: &TypeTag,
    chain_id: ChainId,
) {
    let diem_root_auth_key = AuthenticationKey::ed25519(diem_root_key);
    let treasury_compliance_auth_key = AuthenticationKey::ed25519(treasury_compliance_key);

    let root_diem_root_address = account_config::diem_root_address();
    let tc_account_address = account_config::treasury_compliance_account_address();

    let initial_allow_list = MoveValue::Vector(
        publishing_option
            .script_allow_list
            .into_iter()
            .map(|hash| MoveValue::vector_u8(hash.to_vec().into_iter().collect()))
            .collect(),
    );

    let genesis_gas_schedule = &INITIAL_GAS_SCHEDULE;
    let instr_gas_costs = bcs::to_bytes(&genesis_gas_schedule.instruction_table)
        .expect("Failure serializing genesis instr gas costs");
    let native_gas_costs = bcs::to_bytes(&genesis_gas_schedule.native_table)
        .expect("Failure serializing genesis native gas costs");

    exec_function(
        session,
        log_context,
        GENESIS_MODULE_NAME,
        "initialize",
        vec![],
        serialize_values(&vec![
            MoveValue::Signer(root_diem_root_address),
            MoveValue::Signer(tc_account_address),
            MoveValue::vector_u8(diem_root_auth_key.to_vec()),
            MoveValue::vector_u8(treasury_compliance_auth_key.to_vec()),
            initial_allow_list,
            MoveValue::Bool(publishing_option.is_open_module),
            MoveValue::vector_u8(instr_gas_costs),
            MoveValue::vector_u8(native_gas_costs),
            MoveValue::U8(chain_id.id()),
        ]),
    );

    // Bump the Diem Framework version number
    exec_function(
        session,
        log_context,
        DIEM_VERSION_MODULE_NAME,
        "set",
        vec![],
        serialize_values(&vec![
            MoveValue::Signer(root_diem_root_address),
            MoveValue::U64(
                /* Diem Framework major version number */ DIEM_MAX_KNOWN_VERSION.major,
            ),
        ]),
    );

    // Bump the sequence number for the Association account. If we don't do this and a
    // subsequent transaction (e.g., minting) is sent from the Assocation account, a problem
    // arises: both the genesis transaction and the subsequent transaction have sequence
    // number 0
    exec_function(
        session,
        log_context,
        "DiemAccount",
        "epilogue",
        vec![xdx_ty.clone()],
        serialize_values(&vec![
            MoveValue::Signer(root_diem_root_address),
            MoveValue::U64(/* txn_sequence_number */ 0),
            MoveValue::U64(/* txn_gas_price */ 0),
            MoveValue::U64(/* txn_max_gas_units */ 0),
            MoveValue::U64(/* gas_units_remaining */ 0),
        ]),
    );
}

fn create_and_initialize_testnet_minting(
    session: &mut Session<StateViewCache>,
    log_context: &impl LogContext,
    public_key: &Ed25519PublicKey,
) {
    let genesis_auth_key = AuthenticationKey::ed25519(public_key);
    let create_dd_script = encode_create_designated_dealer_script_function(
        account_config::xus_tag(),
        0,
        account_config::testnet_dd_account_address(),
        genesis_auth_key.prefix().to_vec(),
        b"moneybags".to_vec(), // name
        true,                  // add_all_currencies
    )
    .into_script_function();

    let mint_max_xus = transaction_builder::encode_tiered_mint_script_function(
        account_config::xus_tag(),
        0,
        account_config::testnet_dd_account_address(),
        std::u64::MAX / 2,
        3,
    )
    .into_script_function();

    // Create the DD account
    exec_script_function(
        session,
        log_context,
        account_config::treasury_compliance_account_address(),
        &create_dd_script,
    );

    // mint XUS.
    let treasury_compliance_account_address = account_config::treasury_compliance_account_address();
    exec_script_function(
        session,
        log_context,
        treasury_compliance_account_address,
        &mint_max_xus,
    );

    let testnet_dd_account_address = account_config::testnet_dd_account_address();
    exec_script_function(
        session,
        log_context,
        testnet_dd_account_address,
        &transaction_builder::encode_rotate_authentication_key_script_function(
            genesis_auth_key.to_vec(),
        )
        .into_script_function(),
    );
}

/// Creates and initializes each validator owner and validator operator. This method creates all
/// the required accounts, sets the validator operators for each validator owner, and sets the
/// validator config on-chain.
fn create_and_initialize_owners_operators(
    session: &mut Session<StateViewCache>,
    log_context: &impl LogContext,
    operator_assignments: &[OperatorAssignment],
    operator_registrations: &[OperatorRegistration],
) {
    let diem_root_address = account_config::diem_root_address();

    // Create accounts for each validator owner. The inputs for creating an account are the auth
    // key prefix and account address. Internally move then computes the auth key as auth key
    // prefix || address. Because of this, the initial auth key will be invalid as we produce the
    // account address from the name and not the public key.
    for (owner_key, owner_name, _op_assignment) in operator_assignments {
        let staged_owner_auth_key =
            diem_config::utils::default_validator_owner_auth_key_from_name(owner_name);
        let owner_address = staged_owner_auth_key.derived_address();
        let create_owner_script =
            transaction_builder::encode_create_validator_account_script_function(
                0,
                owner_address,
                staged_owner_auth_key.prefix().to_vec(),
                owner_name.clone(),
            )
            .into_script_function();
        exec_script_function(
            session,
            log_context,
            diem_root_address,
            &create_owner_script,
        );

        // If there is a key, make it the auth key, otherwise use a zero auth key.
        let real_owner_auth_key = if let Some(owner_key) = owner_key {
            AuthenticationKey::ed25519(owner_key).to_vec()
        } else {
            ZERO_AUTH_KEY.to_vec()
        };

        exec_script_function(
            session,
            log_context,
            owner_address,
            &transaction_builder::encode_rotate_authentication_key_script_function(
                real_owner_auth_key,
            )
            .into_script_function(),
        );
    }

    // Create accounts for each validator operator
    for (operator_key, operator_name, _) in operator_registrations {
        let operator_auth_key = AuthenticationKey::ed25519(&operator_key);
        let operator_account = account_address::from_public_key(operator_key);
        let create_operator_script =
            transaction_builder::encode_create_validator_operator_account_script_function(
                0,
                operator_account,
                operator_auth_key.prefix().to_vec(),
                operator_name.clone(),
            )
            .into_script_function();
        exec_script_function(
            session,
            log_context,
            diem_root_address,
            &create_operator_script,
        );
    }

    // Set the validator operator for each validator owner
    for (_owner_key, owner_name, op_assignment) in operator_assignments {
        let owner_address = diem_config::utils::validator_owner_account_from_name(owner_name);
        exec_script_function(session, log_context, owner_address, op_assignment);
    }

    // Set the validator config for each validator
    for (operator_key, _, registration) in operator_registrations {
        let operator_account = account_address::from_public_key(operator_key);
        exec_script_function(session, log_context, operator_account, registration);
    }

    // Add each validator to the validator set
    for (_owner_key, owner_name, _op_assignment) in operator_assignments {
        let owner_address = diem_config::utils::validator_owner_account_from_name(owner_name);
        exec_function(
            session,
            log_context,
            "DiemSystem",
            "add_validator",
            vec![],
            serialize_values(&vec![
                MoveValue::Signer(diem_root_address),
                MoveValue::Address(owner_address),
            ]),
        );
    }
}

/// Publish the standard library.
fn publish_stdlib(
    session: &mut Session<StateViewCache>,
    log_context: &impl LogContext,
    stdlib: Modules,
) {
    let dep_graph = stdlib.compute_dependency_graph();
    for module in dep_graph.compute_topological_order().unwrap() {
        let module_id = module.self_id();
        if module_id.name().as_str() == GENESIS_MODULE_NAME {
            // Do not publish the Genesis module
            continue;
        }
        let mut bytes = vec![];
        module.serialize(&mut bytes).unwrap();
        session
            .publish_module(
                bytes,
                *module_id.address(),
                &mut GasStatus::new_unmetered(),
                log_context,
            )
            .unwrap_or_else(|e| panic!("Failure publishing module {:?}, {:?}", module_id, e));
    }
}

/// Trigger a reconfiguration. This emits an event that will be passed along to the storage layer.
fn reconfigure(session: &mut Session<StateViewCache>, log_context: &impl LogContext) {
    exec_function(
        session,
        log_context,
        "DiemConfig",
        "emit_genesis_reconfiguration_event",
        vec![],
        vec![],
    );
}

/// Verify the consistency of the genesis `WriteSet`
fn verify_genesis_write_set(events: &[ContractEvent]) {
    // (1) first event is account creation event for DiemRoot
    let create_diem_root_event = &events[0];
    assert_eq!(
        *create_diem_root_event.key(),
        CreateAccountEvent::event_key(),
    );

    // (2) second event is account creation event for TreasuryCompliance
    let create_treasury_compliance_event = &events[1];
    assert_eq!(
        *create_treasury_compliance_event.key(),
        CreateAccountEvent::event_key(),
    );

    // (3) The first non-account creation event should be the new epoch event
    let new_epoch_events: Vec<&ContractEvent> = events
        .iter()
        .filter(|e| e.key() == &NewEpochEvent::event_key())
        .collect();
    assert!(
        new_epoch_events.len() == 1,
        "There should only be one NewEpochEvent"
    );
    // (4) This should be the first new_epoch_event
    assert_eq!(new_epoch_events[0].sequence_number(), 0,);
}

/// An enum specifying whether the compiled stdlib/scripts should be used or freshly built versions
/// should be used.
#[derive(Debug, Eq, PartialEq)]
pub enum GenesisOptions {
    Compiled,
    Fresh,
}

/// Generate an artificial genesis `ChangeSet` for testing
pub fn generate_genesis_change_set_for_testing(genesis_options: GenesisOptions) -> ChangeSet {
    let modules = match genesis_options {
        GenesisOptions::Compiled => diem_framework_releases::current_module_blobs(),
        GenesisOptions::Fresh => diem_framework::module_blobs(),
    };

    generate_test_genesis(modules, VMPublishingOption::open(), None).0
}

pub fn test_genesis_transaction() -> Transaction {
    let changeset = test_genesis_change_set_and_validators(None).0;
    Transaction::GenesisTransaction(WriteSetPayload::Direct(changeset))
}

pub fn test_genesis_change_set_and_validators(count: Option<usize>) -> (ChangeSet, Vec<Validator>) {
    generate_test_genesis(
        &current_module_blobs(),
        VMPublishingOption::locked(LegacyStdlibScript::allowlist()),
        count,
    )
}

pub struct Validator {
    pub index: usize,
    pub key: Ed25519PrivateKey,
    pub name: Vec<u8>,
    pub operator_address: AccountAddress,
    pub owner_address: AccountAddress,
}

impl Validator {
    pub fn new_set(count: Option<usize>) -> Vec<Validator> {
        let mut rng: rand::rngs::StdRng = rand::SeedableRng::from_seed([1u8; 32]);
        (0..count.unwrap_or(10))
            .map(|idx| Validator::gen(idx, &mut rng))
            .collect()
    }

    fn gen(index: usize, rng: &mut rand::rngs::StdRng) -> Self {
        let name = index.to_string().as_bytes().to_vec();
        let key = Ed25519PrivateKey::generate(rng);
        let operator_address = account_address::from_public_key(&key.public_key());
        let owner_address = diem_config::utils::validator_owner_account_from_name(&name);

        Self {
            index,
            key,
            name,
            operator_address,
            owner_address,
        }
    }

    fn operator_assignment(&self) -> OperatorAssignment {
        let script_function = transaction_builder::encode_set_validator_operator_script_function(
            self.name.clone(),
            self.operator_address,
        )
        .into_script_function();
        (
            Some(self.key.public_key()),
            self.name.clone(),
            script_function,
        )
    }

    fn operator_registration(&self) -> OperatorRegistration {
        let script_function =
            transaction_builder::encode_register_validator_config_script_function(
                self.owner_address,
                self.key.public_key().to_bytes().to_vec(),
                bcs::to_bytes(&[0u8; 0]).unwrap(),
                bcs::to_bytes(&[0u8; 0]).unwrap(),
            )
            .into_script_function();
        (self.key.public_key(), self.name.clone(), script_function)
    }
}

pub fn generate_test_genesis(
    stdlib_modules: &[Vec<u8>],
    vm_publishing_option: VMPublishingOption,
    count: Option<usize>,
) -> (ChangeSet, Vec<Validator>) {
    let validators = Validator::new_set(count);

    let genesis = encode_genesis_change_set(
        &GENESIS_KEYPAIR.1,
        &GENESIS_KEYPAIR.1,
        &validators
            .iter()
            .map(|v| v.operator_assignment())
            .collect::<Vec<_>>(),
        &validators
            .iter()
            .map(|v| v.operator_registration())
            .collect::<Vec<_>>(),
        stdlib_modules,
        vm_publishing_option,
        ChainId::test(),
    );
    (genesis, validators)
}
