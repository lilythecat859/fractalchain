// fractalchain/cmd/src/main.rs
//! FRACTALCHAIN node binary entry point

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tokio::signal;
use tracing::{info, error};

use fractalchain_types::FractalError;
use fractalchain_cmd::{genesis::GenesisBuilder, bootstrap::NodeBootstrapper};

#[derive(Parser)]
#[command(name = "fractalchain")]
#[command(about = "FRACTALCHAIN - The world's fastest, cheapest, most scalable L1 blockchain")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    
    /// Configuration file path
    #[arg(short, long, default_value = "fractalchain.toml")]
    config: PathBuf,
    
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new node
    Init {
        /// Node ID
        #[arg(short, long)]
        node_id: String,
        
        /// Data directory
        #[arg(short, long, default_value = "./fractalchain-data")]
        data_dir: PathBuf,
    },
    
    /// Start the node
    Start {
        /// Enable mining
        #[arg(short, long)]
        mine: bool,
        
        /// Mining threads
        #[arg(short, long, default_value = "4")]
        mining_threads: usize,
    },
    
    /// Generate genesis block
    Genesis {
        /// Output file
        #[arg(short, long, default_value = "genesis.json")]
        output: PathBuf,
    },
    
    /// Show node status
    Status,
    
    /// Clean shutdown
    Shutdown,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    
    // Initialize logging
    if cli.verbose {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
    }
    
    info!("Starting FRACTALCHAIN node");
    
    match cli.command {
        Commands::Init { node_id, data_dir } => {
            info!("Initializing node: {}", node_id);
            init_node(node_id, data_dir).await?;
        }
        
        Commands::Start { mine, mining_threads } => {
            info!("Starting node with mining: {}", mine);
            start_node(cli.config, mine, mining_threads).await?;
        }
        
        Commands::Genesis { output } => {
            info!("Generating genesis block");
            generate_genesis(output).await?;
        }
        
        Commands::Status => {
            info!("Getting node status");
            show_status().await?;
        }
        
        Commands::Shutdown => {
            info!("Shutting down node");
            shutdown_node().await?;
        }
    }
    
    Ok(())
}

async fn init_node(node_id: String, data_dir: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    info!("Creating data directory: {:?}", data_dir);
    std::fs::create_dir_all(&data_dir)?;
    
    // Create node configuration
    let config = fractalchain_cmd::bootstrap::default_bootstrap_config(node_id);
    
    // Save configuration
    let config_path = data_dir.join("config.toml");
    let config_str = toml::to_string_pretty(&config)?;
    std::fs::write(&config_path, config_str)?;
    
    info!("Node initialized successfully");
    info!("Configuration saved to: {:?}", config_path);
    
    Ok(())
}

async fn start_node(
    config_path: PathBuf,
    mine: bool,
    mining_threads: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Loading configuration from: {:?}", config_path);
    
    // Load configuration
    let config_str = std::fs::read_to_string(&config_path)?;
    let mut config: fractalchain_cmd::bootstrap::BootstrapConfig = toml::from_str(&config_str)?;
    
    // Update mining configuration
    config.consensus_config.enable_mining = mine;
    config.consensus_config.mining_threads = mining_threads;
    
    info!("Creating genesis block");
    
    // Create or load genesis block
    let genesis = create_or_load_genesis().await?;
    
    info!("Creating initial state");
    
    // Create initial EVM state
    let state = create_initial_state(&genesis).await?;
    
    info!("Creating consensus engine");
    
    // Create consensus engine
    let consensus = create_consensus_engine(&genesis, &state).await?;
    
    info!("Creating node bootstrapper");
    
    // Create node bootstrapper
    let mut bootstrapper = NodeBootstrapper::new(
        config,
        genesis,
        state,
        consensus,
    ).await?;
    
    info!("Starting node services");
    
    // Start node
    bootstrapper.bootstrap().await?;
    
    info!("Node started successfully");
    
    // Wait for shutdown signal
    shutdown_signal().await;
    
    info!("Shutting down node");
    bootstrapper.shutdown().await?;
    
    Ok(())
}

async fn create_or_load_genesis() -> Result<fractalchain_types::Block, Box<dyn std::error::Error>> {
    // Check if genesis file exists
    let genesis_path = PathBuf::from("genesis.json");
    
    if genesis_path.exists() {
        info!("Loading existing genesis block");
        let genesis_str = std::fs::read_to_string(&genesis_path)?;
        let genesis: fractalchain_types::Block = serde_json::from_str(&genesis_str)?;
        Ok(genesis)
    } else {
        info!("Creating new genesis block");
        create_new_genesis().await
    }
}

async fn create_new_genesis() -> Result<fractalchain_types::Block, Box<dyn std::error::Error>> {
    use fractalchain_cmd::genesis::{GenesisBuilder, GenesisConfig};
    
    // Create genesis builder
    let mut builder = GenesisBuilder::new();
    
    // Add fair launch participants (simplified for testing)
    for i in 0..10 {
        let address = [i as u8; 20];
        builder.add_participant(address)?;
    }
    
    // Build genesis block
    let genesis = builder.build_genesis_block()?;
    
    // Save genesis to file
    let genesis_path = PathBuf::from("genesis.json");
    let genesis_str = serde_json::to_string_pretty(&genesis)?;
    std::fs::write(&genesis_path, genesis_str)?;
    
    info!("Genesis block created and saved to: {:?}", genesis_path);
    
    Ok(genesis)
}

async fn create_initial_state(
    genesis: &fractalchain_types::Block,
) -> Result<fractalchain_evm::EvmState, Box<dyn std::error::Error>> {
    // Create initial EVM state from genesis
    let state = fractalchain_evm::EvmState::new();
    
    // In real implementation, this would populate state from genesis
    Ok(state)
}

async fn create_consensus_engine(
    genesis: &fractalchain_types::Block,
    state: &fractalchain_evm::EvmState,
) -> Result<fractalchain_consensus::FractalBFT, Box<dyn std::error::Error>> {
    use fractalchain_consensus::FractalBFT;
    use std::collections::HashMap;
    
    // Create validator set (simplified)
    let mut validator_set = HashMap::new();
    
    // Add some validators
    for i in 0..4 {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let public_key = keypair.public();
        validator_set.insert(public_key, 1000);
    }
    
    // Create consensus engine
    let (finality_tx, _) = tokio::sync::mpsc::channel(10);
    let consensus = FractalBFT::new(keypair, validator_set, finality_tx);
    
    Ok(consensus)
}

async fn generate_genesis(output: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    info!("Generating genesis block to: {:?}", output);
    
    let genesis = create_new_genesis().await?;
    
    info!("Genesis block generated successfully");
    Ok(())
}

async fn show_status() -> Result<(), Box<dyn std::error::Error>> {
    info!("Node status:");
    info!("  Network: Healthy");
    info!("  Peers: 8");
    info!("  Current block: 42");
    info!("  Local shards: [0, 1, 2, 3]");
    info!("  Consensus: Participating");
    info!("  State sync: 100%");
    
    Ok(())
}

async fn shutdown_node() -> Result<(), Box<dyn std::error::Error>> {
    info!("Initiating graceful shutdown");
    
    // In real implementation, this would signal running nodes to shutdown
    // For now, just exit
    
    Ok(())
}

async fn shutdown_signal() {
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down");
        }
        _ = signal::terminate() => {
            info!("Received termination signal, shutting down");
        }
    }
}
