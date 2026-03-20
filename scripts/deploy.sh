# fractalchain/scripts/deploy.sh
#!/bin/bash

set -euo pipefail

# FRACTALCHAIN Deployment Script
# Usage: ./deploy.sh [environment] [action]

ENVIRONMENT=${1:-dev}
ACTION=${2:-deploy}

echo "🚀 FRACTALCHAIN Deployment Script"
echo "Environment: $ENVIRONMENT"
echo "Action: $ACTION"

# Validate environment
case "$ENVIRONMENT" in
    dev|staging|prod)
        echo "Deploying to $ENVIRONMENT environment..."
        ;;
    *)
        echo "Unknown environment: $ENVIRONMENT"
        echo "Available environments: dev, staging, prod"
        exit 1
        ;;
esac

# Perform action
case "$ACTION" in
    build)
        echo "Building for $ENVIRONMENT..."
        ./scripts/build.sh release fractal-math,verkle-trees,state-expiry
        ;;
    test)
        echo "Testing for $ENVIRONMENT..."
        ./scripts/test.sh all fractal-math,verkle-trees,state-expiry
        ;;
    deploy)
        echo "Deploying to $ENVIRONMENT..."
        
        # Build and test
        ./scripts/build.sh release fractal-math,verkle-trees,state-expiry
        ./scripts/test.sh all fractal-math,verkle-trees,state-expiry
        
        # Deploy based on environment
        case "$ENVIRONMENT" in
            dev)
                echo "Deploying to development with Docker Compose..."
                docker-compose up -d
                ;;
            staging|prod)
                echo "Deploying to Kubernetes..."
                kubectl apply -f kubernetes/
                ;;
        esac
        ;;
    status)
        echo "Checking status for $ENVIRONMENT..."
        
        case "$ENVIRONMENT" in
            dev)
                docker-compose ps
                ;;
            staging|prod)
                kubectl get all -n fractalchain
                ;;
        esac
        ;;
    logs)
        echo "Showing logs for $ENVIRONMENT..."
        
        case "$ENVIRONMENT" in
            dev)
                docker-compose logs -f
                ;;
            staging|prod)
                kubectl logs -f deployment/fractal-bootstrap -n fractalchain
                ;;
        esac
        ;;
    cleanup)
        echo "Cleaning up $ENVIRONMENT..."
        
        case "$ENVIRONMENT" in
            dev)
                docker-compose down -v
                docker system prune -f
                ;;
            staging|prod)
                kubectl delete namespace fractalchain --ignore-not-found=true
                ;;
        esac
        ;;
    *)
        echo "Unknown action: $ACTION"
        echo "Available actions: build, test, deploy, status, logs, cleanup"
        exit 1
        ;;
esac

echo "✅ Deployment script completed successfully!"
echo ""
echo "📊 Performance Targets:"
echo "  - TPS: 10M+"
echo "  - Latency: <100ms"
echo "  - Finality: <750ms"
echo "  - Cross-shard: <100ms"
echo ""
echo "🔗 Endpoints:"
echo "  - RPC: http://localhost/rpc"
echo "  - Metrics: http://localhost:3000"
echo "  - Logs: See deployment logs above"