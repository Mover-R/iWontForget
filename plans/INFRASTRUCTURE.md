# iWontForget — Infrastructure Learning Roadmap

> **Purpose:** A step-by-step guide to understanding and building the infrastructure for iWontForget. Covers Kubernetes fundamentals, load balancing, inter-service communication, and the exact sequence of infrastructure creation steps.

---

## Table of Contents

1. [Infrastructure Overview](#1-infrastructure-overview)
2. [Kubernetes Fundamentals](#2-kubernetes-fundamentals)
3. [How Requests Flow Through the System](#3-how-requests-flow-through-the-system)
4. [Inter-Service Communication Deep Dive](#4-inter-service-communication-deep-dive)
5. [Load Balancing at Every Layer](#5-load-balancing-at-every-layer)
6. [Infrastructure Components Deep Dive](#6-infrastructure-components-deep-dive)
7. [Step-by-Step Infrastructure Creation](#7-step-by-step-infrastructure-creation)
8. [Local Development Environment](#8-local-development-environment)
9. [Production Infrastructure Setup](#9-production-infrastructure-setup)
10. [Networking and Service Discovery](#10-networking-and-service-discovery)
11. [Secrets and Configuration Management](#11-secrets-and-configuration-management)
12. [Observability Stack](#12-observability-stack)
13. [Troubleshooting Playbook](#13-troubleshooting-playbook)
14. [Learning Path](#14-learning-path)

---

## 1. Infrastructure Overview

### What We Are Building

```mermaid
graph TB
    subgraph INTERNET["Internet"]
        USER["Telegram Users"]
        TG_API["Telegram API Servers"]
    end

    subgraph CLOUD["Cloud Provider - Yandex Cloud / any K8s"]
        LB["L4 Load Balancer<br/>TCP passthrough"]

        subgraph K8S["Kubernetes Cluster"]
            subgraph INGRESS_NS["Namespace: ingress-nginx"]
                NGINX1["NGINX Pod 1"]
                NGINX2["NGINX Pod 2"]
            end

            subgraph APP_NS["Namespace: iwontforget"]
                BG["Bot Gateway x2"]
                US["User Service x2"]
                WS["Wishlist Service x2"]
                RS["Reminder Service x2"]
                FS["Friends Service x2"]
                GS["Gift Service x2"]
                ES["Event Service x2"]
                AI["AI Service x2"]
            end

            subgraph INFRA_NS["Namespace: infrastructure"]
                PG["App PostgreSQL<br/>Primary + Replica"]
                KAFKA["Kafka<br/>3 brokers"]
                REDIS["Redis<br/>Sentinel HA"]
            end

            subgraph TEMPORAL_NS["Namespace: temporal"]
                TEMP["Temporal Server"]
                TEMP_PG["Temporal PostgreSQL<br/>Dedicated"]
                TEMP_UI["Temporal UI"]
            end

            subgraph OBS_NS["Namespace: observability"]
                PROM["Prometheus"]
                GRAF["Grafana"]
                JAEGER["Jaeger"]
                LOKI["Loki"]
            end
        end
    end

    USER <--> TG_API
    TG_API -->|Webhook POST| LB
    LB --> NGINX1
    LB --> NGINX2
    NGINX1 --> BG
    NGINX2 --> BG
    BG --> US
    BG --> WS
    BG --> RS
    BG --> FS
    BG --> AI
```

### Component Count Summary

| Category | Component | Instances | Why |
|----------|-----------|-----------|-----|
| **Load Balancer** | Cloud L4 LB | 1 | Entry point from internet |
| **Ingress** | NGINX Ingress Controller | 2 pods | HA, TLS termination |
| **Application** | 8 microservices | 2 pods each (16 total) | HA, rolling updates |
| **Database** | App PostgreSQL | 1 primary + 1 replica | HA, read scaling |
| **Database** | Temporal PostgreSQL | 1 primary + 1 replica | Isolated from app |
| **Message Broker** | Kafka | 3 brokers | Replication factor 3 |
| **Cache** | Redis | 1 master + 2 replicas + 3 sentinels | HA via Sentinel |
| **Workflow Engine** | Temporal | 1-2 frontend + 1-2 history + 1-2 matching | Scalable components |
| **Observability** | Prometheus + Grafana + Jaeger + Loki | 1 each | Monitoring stack |

---

## 2. Kubernetes Fundamentals

### 2.1. What is Kubernetes?

Kubernetes (K8s) is a container orchestration platform. Instead of manually deploying services on servers, you describe the **desired state** (how many replicas, what resources, what health checks) and K8s makes it happen.

### 2.2. Key Concepts for iWontForget

```mermaid
graph TD
    subgraph CLUSTER["K8s Cluster"]
        subgraph NODE1["Node 1 - VM"]
            POD1["Pod: bot-gateway-abc123"]
            POD2["Pod: user-service-def456"]
        end
        subgraph NODE2["Node 2 - VM"]
            POD3["Pod: bot-gateway-xyz789"]
            POD4["Pod: wishlist-service-ghi012"]
        end
    end

    SVC["Service: bot-gateway<br/>ClusterIP: 10.96.0.15:50051"]
    SVC -->|routes to| POD1
    SVC -->|routes to| POD3

    DEPLOY["Deployment: bot-gateway<br/>replicas: 2"]
    DEPLOY -.->|manages| POD1
    DEPLOY -.->|manages| POD3
```

| Concept | What It Is | iWontForget Example |
|---------|-----------|---------------------|
| **Pod** | Smallest deployable unit. One or more containers sharing network/storage. | One instance of `wishlist-service` running in a container |
| **Deployment** | Declares desired state: image, replicas, resources, probes. K8s ensures this state. | "I want 2 replicas of wishlist-service with 128Mi RAM" |
| **Service** | Stable network endpoint (virtual IP) that routes to healthy pods. | `wishlist-service.iwontforget.svc:50051` — always reachable even if pods restart |
| **Namespace** | Logical isolation within a cluster. Like folders for K8s resources. | `iwontforget` for app, `infrastructure` for DBs, `temporal` for Temporal |
| **ConfigMap** | Non-sensitive configuration data. | Service config YAML, feature flags |
| **Secret** | Sensitive data (passwords, API keys). Encrypted at rest. | DB credentials, Telegram bot token, LLM API keys |
| **Ingress** | HTTP/gRPC routing rules from outside the cluster to services inside. | Route `/webhook` to Bot Gateway |
| **HPA** | Horizontal Pod Autoscaler — auto-scales pods based on CPU/memory. | Scale wishlist-service from 2 to 5 pods when CPU > 70% |
| **PDB** | Pod Disruption Budget — ensures minimum availability during updates. | "Always keep at least 1 wishlist-service pod running" |
| **PV/PVC** | Persistent Volume / Claim — durable storage for stateful workloads. | PostgreSQL data directory, Kafka log segments |

### 2.3. Namespaces Strategy

```
k8s-cluster/
├── ingress-nginx/          # NGINX Ingress Controller
├── iwontforget/            # All application microservices
│   ├── bot-gateway
│   ├── user-service
│   ├── wishlist-service
│   ├── reminder-service
│   ├── friends-service
│   ├── gift-service
│   ├── event-service
│   └── ai-service
├── infrastructure/         # Shared infrastructure
│   ├── postgresql (App)
│   ├── kafka (Strimzi)
│   └── redis (Sentinel)
├── temporal/               # Temporal + its own PostgreSQL
│   ├── temporal-server
│   ├── temporal-postgresql
│   └── temporal-ui
├── observability/          # Monitoring stack
│   ├── prometheus
│   ├── grafana
│   ├── jaeger
│   └── loki
└── cert-manager/           # TLS certificate automation
```

**Why namespaces?**
- **Resource isolation:** Set CPU/memory quotas per namespace
- **RBAC:** Different permissions per namespace (devs can't touch infrastructure)
- **Network policies:** Control which namespaces can talk to each other
- **Organization:** `kubectl get pods -n iwontforget` shows only app services

### 2.4. Essential kubectl Commands

```bash
# ─── Cluster Info ───
kubectl cluster-info                          # Cluster endpoint
kubectl get nodes                             # List cluster nodes
kubectl top nodes                             # Node resource usage

# ─── Namespaces ───
kubectl get namespaces                        # List all namespaces
kubectl create namespace iwontforget          # Create namespace

# ─── Pods ───
kubectl get pods -n iwontforget               # List pods in namespace
kubectl get pods -n iwontforget -o wide       # With node and IP info
kubectl describe pod <pod-name> -n iwontforget # Detailed pod info
kubectl logs <pod-name> -n iwontforget        # Pod logs
kubectl logs <pod-name> -n iwontforget -f     # Stream logs
kubectl logs <pod-name> -n iwontforget --previous  # Logs from crashed container

# ─── Deployments ───
kubectl get deployments -n iwontforget        # List deployments
kubectl scale deployment wishlist-service --replicas=3 -n iwontforget  # Manual scale
kubectl rollout status deployment/wishlist-service -n iwontforget      # Watch rollout
kubectl rollout undo deployment/wishlist-service -n iwontforget        # Rollback

# ─── Services ───
kubectl get svc -n iwontforget                # List services
kubectl get endpoints wishlist-service -n iwontforget  # Which pods back this service

# ─── Debugging ───
kubectl exec -it <pod-name> -n iwontforget -- /bin/sh  # Shell into pod
kubectl port-forward svc/wishlist-service 50051:50051 -n iwontforget  # Local access
kubectl get events -n iwontforget --sort-by='.lastTimestamp'  # Recent events

# ─── Apply manifests ───
kubectl apply -f deployment.yaml              # Create/update resources
kubectl delete -f deployment.yaml             # Delete resources
kubectl diff -f deployment.yaml               # Preview changes
```

---

## 3. How Requests Flow Through the System

### 3.1. Incoming Request: User Sends a Message

```mermaid
sequenceDiagram
    participant U as Telegram User
    participant TG as Telegram Servers
    participant LB as Cloud L4 Load Balancer
    participant NG as NGINX Ingress
    participant BG as Bot Gateway Pod
    participant US as User Service Pod
    participant WS as Wishlist Service Pod
    participant PG as PostgreSQL
    participant KF as Kafka

    U->>TG: Send message: Хочу AirPods
    TG->>LB: POST /webhook with Update JSON
    Note over LB: TCP round-robin to NGINX pod
    LB->>NG: Forward TCP connection
    Note over NG: TLS termination, route /webhook to bot-gateway service
    NG->>BG: HTTP POST /webhook
    Note over BG: Parse Update, extract user_id and text

    BG->>US: gRPC GetOrCreateUser
    Note over US: K8s Service routes to healthy pod
    US->>PG: SELECT/INSERT user
    PG-->>US: User record
    US-->>BG: User response

    Note over BG: Classify message as wish

    BG->>WS: gRPC CreateWish
    Note over WS: K8s Service routes to healthy pod
    WS->>PG: BEGIN transaction
    WS->>PG: INSERT INTO wishes
    WS->>PG: INSERT INTO outbox_events
    WS->>PG: COMMIT
    PG-->>WS: OK
    WS-->>BG: Wish created

    BG->>TG: sendMessage: Added wish AirPods
    TG-->>U: Display confirmation

    Note over WS: Outbox relay goroutine runs every 500ms
    WS->>KF: Publish wish.created event
    WS->>PG: UPDATE outbox_events SET published_at = NOW
```

### 3.2. Outgoing Notification: Reminder Fires

```mermaid
sequenceDiagram
    participant T as Temporal Server
    participant RW as Reminder Worker Pod
    participant BG as Bot Gateway Pod
    participant RD as Redis
    participant TG as Telegram API

    Note over T: Timer fires for reminder
    T->>RW: Execute SendNotification activity
    RW->>BG: gRPC SendNotification
    
    BG->>RD: Check dedup key in DB2
    RD-->>BG: Not duplicate
    
    BG->>RD: Check rate limit in DB1
    RD-->>BG: Within limits
    
    BG->>TG: sendMessage API call
    TG-->>BG: 200 OK
    
    BG->>RD: Store dedup key with 1h TTL
    BG-->>RW: Success
    RW-->>T: Activity completed
```

### 3.3. DNS Resolution Path

When `bot-gateway` calls `wishlist-service`, here is what happens at the network level:

```
1. Code calls: grpc.Dial("wishlist-service.iwontforget.svc.cluster.local:50051")

2. DNS resolution (CoreDNS inside K8s):
   wishlist-service.iwontforget.svc.cluster.local → 10.96.45.123 (ClusterIP)

3. kube-proxy (iptables/IPVS on the node):
   10.96.45.123:50051 → one of:
     - 10.244.1.15:50051 (wishlist-service pod on node 1)
     - 10.244.2.23:50051 (wishlist-service pod on node 2)

4. TCP connection established directly to the selected pod IP
5. gRPC request sent over this connection
```

**Short DNS names work within the same namespace:**
```go
// Within iwontforget namespace, these are equivalent:
"wishlist-service:50051"                                    // Short name (same namespace)
"wishlist-service.iwontforget:50051"                        // With namespace
"wishlist-service.iwontforget.svc:50051"                    // With svc
"wishlist-service.iwontforget.svc.cluster.local:50051"      // FQDN
```

**Cross-namespace calls require at least the namespace:**
```go
// From iwontforget namespace to infrastructure namespace:
"app-postgresql-rw.infrastructure.svc:5432"                 // PostgreSQL
"kafka-bootstrap.infrastructure.svc:9092"                   // Kafka
"redis.infrastructure.svc:6379"                             // Redis

// From iwontforget namespace to temporal namespace:
"temporal-frontend.temporal.svc:7233"                       // Temporal
```

---

## 4. Inter-Service Communication Deep Dive

### 4.1. Communication Patterns

```mermaid
graph LR
    subgraph SYNC["Synchronous - gRPC"]
        BG[Bot Gateway] -->|Request/Response| US[User Service]
        BG -->|Request/Response| WS[Wishlist Service]
        BG -->|Request/Response| RS[Reminder Service]
        BG -->|Request/Response| AI[AI Service]
    end

    subgraph ASYNC["Asynchronous - Kafka"]
        WS2[Wishlist Service] -->|Events| KF[Kafka]
        FS[Friends Service] -->|Events| KF
    end

    subgraph WORKFLOW["Durable Workflows - Temporal"]
        RS2[Reminder Service] -->|Start workflow| T[Temporal]
        T -->|Execute activity| BG2[Bot Gateway]
    end
```

### 4.2. gRPC: How Services Talk to Each Other

**What is gRPC?** A high-performance RPC framework using Protocol Buffers for serialization and HTTP/2 for transport.

**Why gRPC over REST?**
| Aspect | REST/JSON | gRPC/Protobuf |
|--------|-----------|---------------|
| Serialization | JSON (text, slow) | Protobuf (binary, fast) |
| Schema | OpenAPI (optional) | `.proto` files (mandatory) |
| Streaming | Workarounds (SSE, WebSocket) | Native bidirectional streaming |
| Code generation | Optional (swagger-codegen) | Built-in (protoc) |
| Type safety | Runtime validation | Compile-time type checking |
| HTTP version | HTTP/1.1 | HTTP/2 (multiplexing) |

**How a gRPC call works in our system:**

```go
// ─── 1. Define the contract (proto file) ───
// proto/wishlist/v1/wishlist.proto
service WishlistService {
  rpc CreateWish(CreateWishRequest) returns (Wish);
}

// ─── 2. Generate Go code (via buf generate) ───
// This creates:
//   - wishlist/v1/wishlist.pb.go       (message types)
//   - wishlist/v1/wishlist_grpc.pb.go  (client + server interfaces)

// ─── 3. Server side (wishlist-service) ───
type wishlistServer struct {
    pb.UnimplementedWishlistServiceServer
    repo WishRepository
}

func (s *wishlistServer) CreateWish(ctx context.Context, req *pb.CreateWishRequest) (*pb.Wish, error) {
    wish, err := s.repo.Create(ctx, req)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "failed to create wish: %v", err)
    }
    return wish, nil
}

// Start server
lis, _ := net.Listen("tcp", ":50051")
grpcServer := grpc.NewServer(
    grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
        grpc_recovery.UnaryServerInterceptor(),
        grpc_prometheus.UnaryServerInterceptor,
        otelgrpc.UnaryServerInterceptor(),
    )),
)
pb.RegisterWishlistServiceServer(grpcServer, &wishlistServer{})
grpcServer.Serve(lis)

// ─── 4. Client side (Go service calling another Go service) ───
conn, _ := grpc.Dial(
    "wishlist-service.iwontforget.svc:50051",
    grpc.WithTransportCredentials(insecure.NewCredentials()), // TLS inside cluster optional
    grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
    grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
        grpc_retry.UnaryClientInterceptor(
            grpc_retry.WithMax(3),
            grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100*time.Millisecond)),
        ),
        grpc_prometheus.UnaryClientInterceptor,
        otelgrpc.UnaryClientInterceptor(),
    )),
)
client := pb.NewWishlistServiceClient(conn)

// Make the call
wish, err := client.CreateWish(ctx, &pb.CreateWishRequest{
    UserId: 12345,
    Title:  "AirPods Pro",
})
```

> **Note:** Bot Gateway uses the same protobuf definitions but with Python's `grpcio` library.
> The gRPC protocol is language-agnostic — Python and Go services communicate seamlessly.

```python
# ─── 4b. Client side (Bot Gateway — Python calling Go service) ───
import grpc
from proto.wishlist.v1 import wishlist_pb2, wishlist_pb2_grpc

channel = grpc.aio.insecure_channel(
    "wishlist-service.iwontforget.svc:50051",
    options=[
        ("grpc.keepalive_time_ms", 10000),
        ("grpc.keepalive_timeout_ms", 5000),
    ],
)
stub = wishlist_pb2_grpc.WishlistServiceStub(channel)

# Make the call — same protobuf contract, different language
wish = await stub.CreateWish(wishlist_pb2.CreateWishRequest(
    user_id=12345,
    title="AirPods Pro",
))
```

### 4.3. gRPC Interceptors (Middleware)

Every gRPC call passes through interceptors — like middleware in HTTP frameworks:

```mermaid
graph LR
    REQ[gRPC Request] --> TRACE[OpenTelemetry<br/>Tracing]
    TRACE --> METRICS[Prometheus<br/>Metrics]
    METRICS --> LOG[Structured<br/>Logging]
    LOG --> RETRY[Retry<br/>on failure]
    RETRY --> HANDLER[Business<br/>Logic]
    HANDLER --> RESP[gRPC Response]
```

| Interceptor | Purpose | Library |
|-------------|---------|---------|
| **Recovery** | Catch panics, return Internal error | `grpc_recovery` |
| **Prometheus** | Request count, latency histograms | `grpc_prometheus` |
| **OpenTelemetry** | Distributed tracing (trace ID propagation) | `otelgrpc` |
| **Logging** | Structured request/response logging | `grpc_zap` |
| **Retry** | Client-side retry with exponential backoff | `grpc_retry` |
| **Auth** | Validate JWT/API key from metadata | Custom |

### 4.4. Kafka: Asynchronous Event Log

**What is Kafka?** A distributed event streaming platform. Think of it as an append-only log that multiple consumers can read from independently.

**How Kafka works in iWontForget:**

```mermaid
graph LR
    subgraph PRODUCER["Producer: Wishlist Service"]
        TX["DB Transaction"]
        OB["Outbox Table"]
        RELAY["Outbox Relay Goroutine"]
    end

    subgraph KAFKA["Kafka Cluster"]
        T1["Topic: wish.events<br/>Partition 0"]
        T2["Topic: wish.events<br/>Partition 1"]
        T3["Topic: wish.events<br/>Partition 2"]
    end

    subgraph CONSUMER["Consumer: Analytics - future"]
        CG["Consumer Group"]
    end

    TX -->|1. Write wish + event in same TX| OB
    RELAY -->|2. Poll unpublished events| OB
    RELAY -->|3. Publish to Kafka| T1
    RELAY -->|3. Publish to Kafka| T2
    CG -->|4. Read events| T1
    CG -->|4. Read events| T2
```

**Key Kafka concepts:**

| Concept | Explanation | Our Usage |
|---------|-------------|-----------|
| **Topic** | Named stream of events | `wish.events`, `reminder.events`, `friend.events`, `gift.events`, `event.events` |
| **Partition** | Ordered sub-stream within a topic. Enables parallelism. | 3 partitions per topic (matches broker count) |
| **Producer** | Writes events to topics | Outbox relay goroutine in each service |
| **Consumer** | Reads events from topics | Future analytics, cross-service reactions |
| **Consumer Group** | Set of consumers that share partitions. Each message delivered to one consumer in the group. | One group per consuming service |
| **Offset** | Position in a partition. Consumers track their offset. | Committed after processing |
| **Replication Factor** | How many brokers store a copy of each partition | 3 (every broker has every partition) |
| **Retention** | How long events are kept | 7 days (configurable) |

### 4.5. Temporal: Durable Workflow Orchestration

**What is Temporal?** A platform for writing durable, long-running workflows. If a process crashes mid-execution, Temporal resumes it exactly where it left off.

**How Temporal works in iWontForget:**

```mermaid
sequenceDiagram
    participant RS as Reminder Service
    participant TF as Temporal Frontend
    participant TH as Temporal History
    participant TPG as Temporal PostgreSQL
    participant TW as Temporal Worker<br/>in Reminder Service
    participant BG as Bot Gateway

    RS->>TF: StartWorkflow ReminderWorkflow
    TF->>TH: Create workflow execution
    TH->>TPG: Persist workflow state

    Note over TH: Workflow code runs: timer for 2 hours

    TH->>TPG: Schedule timer task
    Note over TPG: 2 hours pass...
    TPG-->>TH: Timer fires

    TH->>TW: Dispatch SendNotification activity
    TW->>BG: gRPC SendNotification
    BG-->>TW: Success
    TW-->>TH: Activity completed
    TH->>TPG: Persist completion
```

**Key Temporal concepts:**

| Concept | Explanation | Our Usage |
|---------|-------------|-----------|
| **Workflow** | A function that orchestrates activities. Durable — survives crashes. | `ReminderWorkflow`, `GiftGroupWorkflow`, `EventReminderWorkflow` |
| **Activity** | A function that does actual work (DB call, API call). Can fail and retry. | `SendNotification`, `UpdateStatus`, `TallyVotes` |
| **Worker** | A process that polls Temporal for tasks and executes workflows/activities. | Each service runs its own Temporal worker |
| **Task Queue** | Named queue that workers poll. Routes work to the right service. | `reminder-service-queue`, `gift-service-queue` |
| **Signal** | External input to a running workflow. | User snoozes a reminder, organizer starts voting |
| **Timer** | Durable sleep — workflow pauses for a duration, survives restarts. | "Wait 2 hours then send reminder" |
| **Schedule** | Cron-like recurring workflow execution. | Daily birthday check at 09:00 |

---

## 5. Load Balancing at Every Layer

### 5.1. The Full Load Balancing Chain

```mermaid
graph TD
    USER["Telegram API<br/>sends webhook"] -->|HTTPS| LB

    subgraph L4["Layer 4: Cloud Load Balancer"]
        LB["TCP Load Balancer<br/>Round-robin<br/>Health: TCP port check"]
    end

    LB -->|TCP| NGINX1["NGINX Pod 1"]
    LB -->|TCP| NGINX2["NGINX Pod 2"]

    subgraph L7["Layer 7: NGINX Ingress"]
        NGINX1
        NGINX2
    end

    NGINX1 -->|HTTP| BG_SVC
    NGINX2 -->|HTTP| BG_SVC

    subgraph K8S_SVC["Layer 3-4: K8s Service - kube-proxy"]
        BG_SVC["Service: bot-gateway<br/>ClusterIP: 10.96.0.15<br/>iptables/IPVS round-robin"]
    end

    BG_SVC --> BG1["bot-gateway pod 1<br/>10.244.1.15"]
    BG_SVC --> BG2["bot-gateway pod 2<br/>10.244.2.23"]

    BG1 -->|gRPC| WS_SVC
    BG2 -->|gRPC| WS_SVC

    subgraph GRPC_LB["Layer 7: gRPC Client-Side LB"]
        WS_SVC["Service: wishlist-service<br/>dns:/// resolver<br/>round-robin with health check"]
    end

    WS_SVC --> WS1["wishlist-service pod 1"]
    WS_SVC --> WS2["wishlist-service pod 2"]
```

### 5.2. Layer-by-Layer Explanation

#### Layer 4: Cloud Load Balancer

**What:** A TCP load balancer provided by the cloud provider (Yandex ALB, AWS NLB, etc.).

**How it works:**
- Receives TCP connections from the internet
- Distributes them to NGINX Ingress pods using round-robin
- Health checks: TCP port probe (is the port open?)
- Does NOT inspect HTTP content — just forwards TCP packets

**Why L4 and not L7?**
- Cheaper and faster (no HTTP parsing)
- NGINX handles L7 routing inside the cluster
- Telegram webhooks are simple POST requests — no complex routing needed

#### Layer 7: NGINX Ingress Controller

**What:** An NGINX instance running inside K8s that routes HTTP/gRPC traffic based on rules.

**How it works:**
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: bot-gateway-ingress
  namespace: iwontforget
  annotations:
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/backend-protocol: "HTTP"
spec:
  ingressClassName: nginx
  tls:
    - hosts:
        - bot.iwontforget.ru
      secretName: bot-tls-cert
  rules:
    - host: bot.iwontforget.ru
      http:
        paths:
          - path: /webhook
            pathType: Prefix
            backend:
              service:
                name: bot-gateway
                port:
                  number: 8443
```

**What NGINX does:**
1. **TLS termination** — decrypts HTTPS, forwards plain HTTP to pods
2. **Path routing** — `/webhook` → bot-gateway service
3. **Rate limiting** — optional, can limit requests per IP
4. **Request buffering** — handles slow clients without tying up backend pods

#### Layer 3-4: Kubernetes Service (ClusterIP)

**What:** A virtual IP address that load-balances across pods.

**How it works:**
```yaml
apiVersion: v1
kind: Service
metadata:
  name: wishlist-service
  namespace: iwontforget
spec:
  type: ClusterIP          # Only accessible within the cluster
  selector:
    app: wishlist-service  # Routes to pods with this label
  ports:
    - port: 50051          # Service port
      targetPort: 50051    # Container port
      protocol: TCP
      name: grpc
```

**Under the hood (kube-proxy):**
- kube-proxy runs on every node
- Creates iptables rules (or IPVS entries) that map ClusterIP → pod IPs
- When a pod connects to `10.96.45.123:50051`, iptables rewrites the destination to a random healthy pod IP
- This is **connection-level** balancing — each new TCP connection goes to a different pod

#### Layer 7: gRPC Client-Side Load Balancing

**Problem:** K8s Service balances at the TCP connection level. But gRPC uses HTTP/2, which **multiplexes** many requests over a single connection. So all requests go to the same pod!

**Solution:** gRPC client-side load balancing with `dns:///` resolver:

```go
conn, _ := grpc.Dial(
    "dns:///wishlist-service.iwontforget.svc:50051",
    grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
)
```

**How it works:**
1. gRPC resolves the DNS name to multiple pod IPs (K8s headless service)
2. Creates a connection to each pod
3. Distributes requests across connections using round-robin
4. Periodically re-resolves DNS to discover new pods

**Headless Service for gRPC:**
```yaml
apiVersion: v1
kind: Service
metadata:
  name: wishlist-service-headless
  namespace: iwontforget
spec:
  type: ClusterIP
  clusterIP: None          # Headless! DNS returns pod IPs directly
  selector:
    app: wishlist-service
  ports:
    - port: 50051
      targetPort: 50051
      name: grpc
```

### 5.3. Load Balancing Summary

| Layer | Component | Algorithm | Granularity | Health Check |
|-------|-----------|-----------|-------------|-------------|
| **L4** | Cloud LB | Round-robin | TCP connection | TCP port probe |
| **L7** | NGINX Ingress | Round-robin | HTTP request | HTTP health endpoint |
| **L3-4** | K8s Service | Random (iptables) | TCP connection | Pod readiness probe |
| **L7** | gRPC client | Round-robin | gRPC request | gRPC health check |
| **Kafka** | Partition-based | Consumer group rebalance | Message | Broker heartbeat |

---

## 6. Infrastructure Components Deep Dive

### 6.1. PostgreSQL (CloudNativePG)

A Kubernetes operator that manages PostgreSQL clusters with automated failover, backups, and replication.

**Two separate clusters:**

| Cluster | Namespace | Purpose | Tuning |
|---------|-----------|---------|--------|
| App PostgreSQL | `infrastructure` | All application service data | Standard OLTP settings |
| Temporal PostgreSQL | `temporal` | Temporal workflow history | High `work_mem`, aggressive vacuum |

**How CloudNativePG works:**

```mermaid
graph LR
    subgraph CNPG["CloudNativePG Operator"]
        OP["Operator Pod"]
    end

    subgraph PG_CLUSTER["PostgreSQL Cluster"]
        PRIMARY["Primary<br/>Read + Write"]
        REPLICA["Replica<br/>Read Only"]
    end

    subgraph K8S_SVCS["K8s Services auto-created"]
        RW["app-postgresql-rw<br/>Always points to primary"]
        RO["app-postgresql-ro<br/>Points to replicas"]
    end

    OP -->|Manages| PRIMARY
    OP -->|Manages| REPLICA
    PRIMARY -->|Streaming replication| REPLICA
    RW --> PRIMARY
    RO --> REPLICA
```

**Key features:**
- **Automatic failover:** If primary dies, replica is promoted in seconds
- **Automated backups:** Scheduled backups to S3/object storage
- **Rolling updates:** Zero-downtime PostgreSQL version upgrades
- **Connection pooling:** Built-in PgBouncer sidecar option

**Service DNS names:**
```
app-postgresql-rw.infrastructure.svc:5432    # Always the primary (read-write)
app-postgresql-ro.infrastructure.svc:5432    # Load-balanced across replicas (read-only)
```

### 6.2. Kafka (Strimzi)

A Kubernetes operator that manages Apache Kafka clusters.

**Key configuration:**
- **KRaft mode** (no ZooKeeper) — simpler, fewer components
- **3 brokers** — minimum for replication factor 3
- **Replication factor 3** — every partition exists on all 3 brokers
- **`min.insync.replicas: 2`** — at least 2 brokers must acknowledge a write

**Service DNS:**
```
kafka-bootstrap.infrastructure.svc:9092      # Bootstrap (discovers all brokers)
```

### 6.3. Redis (Sentinel)

In-memory data store used for caching, sessions, rate limiting, and dedup.

**How Sentinel failover works:**
1. Sentinels continuously ping the master
2. If master does not respond, sentinels vote on whether it is really down
3. If majority agrees (quorum), one sentinel promotes a replica to master
4. All clients are notified of the new master address

**Database allocation:**

| DB | Service | Purpose |
|----|---------|---------|
| DB0 | Bot Gateway | Session state |
| DB1 | Bot Gateway | Rate limiting |
| DB2 | Bot Gateway | Notification dedup |
| DB3 | AI Service | LLM response cache |

### 6.4. Temporal

A durable workflow execution engine. Workflows survive process crashes, server restarts, and infrastructure failures.

**Components:**

| Component | Role | Scaling |
|-----------|------|---------|
| **Frontend** | API gateway, rate limiting, routing | 1-2 replicas |
| **History** | Manages workflow state, timers, activities | 1-2 replicas |
| **Matching** | Task queue management, dispatches tasks to workers | 1-2 replicas |
| **Worker** | Internal Temporal workers | 1 replica |
| **UI** | Web dashboard for workflow visibility | 1 replica |

**Our workers run inside application services:**

```mermaid
graph LR
    TF["Temporal Frontend<br/>temporal.svc:7233"]

    RW["Reminder Service Worker<br/>queue: reminder-service-queue"]
    FW["Friends Service Worker<br/>queue: friends-service-queue"]
    GW["Gift Service Worker<br/>queue: gift-service-queue"]
    EW["Event Service Worker<br/>queue: event-service-queue"]

    RW -->|Poll for tasks| TF
    FW -->|Poll for tasks| TF
    GW -->|Poll for tasks| TF
    EW -->|Poll for tasks| TF
```

---

## 7. Step-by-Step Infrastructure Creation

This is the exact sequence of steps to build the infrastructure from scratch.

### Overview

```mermaid
graph TD
    P0["Phase 0: Prerequisites<br/>Tools + K8s cluster + Namespaces + Ingress"] --> P1
    P1["Phase 1: Data Layer<br/>PostgreSQL + Kafka + Redis"] --> P2
    P2["Phase 2: Temporal<br/>Server + UI + Namespace registration"] --> P3
    P3["Phase 3: Observability<br/>Prometheus + Grafana + Loki + Jaeger"] --> P4
    P4["Phase 4: Application Services<br/>Secrets + Deploy services + Ingress + Webhook"] --> P5
    P5["Phase 5: CI/CD<br/>ArgoCD + GitHub Actions"]
```

### Phase 0: Prerequisites

#### Step 0.1: Install Local Tools

```bash
# Kubernetes CLI
brew install kubectl

# Helm — package manager for K8s
brew install helm

# Local K8s for development
brew install minikube

# Buf — protobuf tooling
brew install bufbuild/buf/buf

# Temporal CLI
brew install temporal

# ArgoCD CLI
brew install argocd
```

#### Step 0.2: Create Kubernetes Cluster

**Local development:**
```bash
minikube start --cpus=4 --memory=8192 --driver=docker
minikube addons enable ingress
minikube addons enable metrics-server
```

**Production (Yandex Cloud):**
```bash
yc managed-kubernetes cluster create \
  --name iwontforget-cluster \
  --network-name default \
  --zone ru-central1-a \
  --public-ip \
  --release-channel regular

yc managed-kubernetes node-group create \
  --cluster-name iwontforget-cluster \
  --name default-pool \
  --platform-id standard-v3 \
  --cores 4 --memory 8 --disk-size 50 \
  --fixed-size 3

yc managed-kubernetes cluster get-credentials iwontforget-cluster --external
```

#### Step 0.3: Create Namespaces

```bash
kubectl create namespace iwontforget
kubectl create namespace infrastructure
kubectl create namespace temporal
kubectl create namespace observability
```

#### Step 0.4: Install cert-manager

```bash
helm repo add jetstack https://charts.jetstack.io && helm repo update
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --set installCRDs=true
```

#### Step 0.5: Install NGINX Ingress Controller

```bash
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx --create-namespace \
  --set controller.replicaCount=2 \
  --set controller.service.type=LoadBalancer
# Note the EXTERNAL-IP from: kubectl get svc -n ingress-nginx
```

### Phase 1: Data Layer

#### Step 1.1: Install CloudNativePG Operator

```bash
kubectl apply --server-side -f \
  https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.22/releases/cnpg-1.22.0.yaml
```

#### Step 1.2: Create App PostgreSQL Cluster

```yaml
# infra/postgresql/app-cluster.yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: app-postgresql
  namespace: infrastructure
spec:
  instances: 2
  postgresql:
    parameters:
      max_connections: "200"
      shared_buffers: "256MB"
  storage:
    size: 10Gi
  bootstrap:
    initdb:
      database: iwontforget
      owner: app
      secret:
        name: app-postgresql-credentials
```

```bash
kubectl create secret generic app-postgresql-credentials \
  --namespace infrastructure \
  --from-literal=username=app \
  --from-literal=password=$(openssl rand -base64 24)

kubectl apply -f infra/postgresql/app-cluster.yaml
kubectl get cluster -n infrastructure -w  # Wait for healthy state

# Create schemas for each service
kubectl exec -it app-postgresql-1 -n infrastructure -- psql -U app -d iwontforget -c "
  CREATE SCHEMA IF NOT EXISTS users;
  CREATE SCHEMA IF NOT EXISTS wishes;
  CREATE SCHEMA IF NOT EXISTS reminders;
  CREATE SCHEMA IF NOT EXISTS friends;
  CREATE SCHEMA IF NOT EXISTS gifts;
  CREATE SCHEMA IF NOT EXISTS events;
"
```

#### Step 1.3: Create Temporal PostgreSQL Cluster

```yaml
# infra/postgresql/temporal-cluster.yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: temporal-postgresql
  namespace: temporal
spec:
  instances: 2
  postgresql:
    parameters:
      max_connections: "300"
      work_mem: "16MB"
      autovacuum_max_workers: "4"
  storage:
    size: 20Gi
  bootstrap:
    initdb:
      database: temporal
      owner: temporal
      secret:
        name: temporal-postgresql-credentials
```

```bash
kubectl create secret generic temporal-postgresql-credentials \
  --namespace temporal \
  --from-literal=username=temporal \
  --from-literal=password=$(openssl rand -base64 24)

kubectl apply -f infra/postgresql/temporal-cluster.yaml
```

#### Step 1.4: Install Strimzi + Create Kafka Cluster

```bash
kubectl apply -f 'https://strimzi.io/install/latest?namespace=infrastructure' -n infrastructure
```

```yaml
# infra/kafka/cluster.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: kafka
  namespace: infrastructure
spec:
  kafka:
    version: 3.7.0
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    config:
      offsets.topic.replication.factor: 3
      default.replication.factor: 3
      min.insync.replicas: 2
      log.retention.hours: 168
    storage:
      type: persistent-claim
      size: 10Gi
  kraft:
    replicas: 3
  entityOperator:
    topicOperator: {}
```

#### Step 1.5: Create Kafka Topics

```yaml
# infra/kafka/topics.yaml — one KafkaTopic per topic
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: wish.events
  namespace: infrastructure
  labels:
    strimzi.io/cluster: kafka
spec:
  partitions: 3
  replicas: 3
  config:
    retention.ms: 604800000
# Repeat for: reminder.events, friend.events, gift.events, event.events
```

#### Step 1.6: Install Redis

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install redis bitnami/redis \
  --namespace infrastructure \
  --set architecture=replication \
  --set sentinel.enabled=true \
  --set replica.replicaCount=2 \
  --set master.persistence.size=2Gi
```

### Phase 2: Temporal

#### Step 2.1: Install Temporal via Helm

```bash
helm repo add temporal https://go.temporal.io/helm-charts

TEMPORAL_PG_HOST="temporal-postgresql-rw.temporal.svc"
TEMPORAL_PG_PASS=$(kubectl get secret temporal-postgresql-credentials \
  -n temporal -o jsonpath='{.data.password}' | base64 -d)

helm install temporal temporal/temporal \
  --namespace temporal \
  --set cassandra.enabled=false \
  --set mysql.enabled=false \
  --set postgresql.enabled=false \
  --set server.config.persistence.default.sql.driver=postgres12 \
  --set server.config.persistence.default.sql.host=$TEMPORAL_PG_HOST \
  --set server.config.persistence.default.sql.port=5432 \
  --set server.config.persistence.default.sql.database=temporal \
  --set server.config.persistence.default.sql.user=temporal \
  --set server.config.persistence.default.sql.password=$TEMPORAL_PG_PASS \
  --set web.enabled=true
```

#### Step 2.2: Register Temporal Namespace

```bash
kubectl port-forward svc/temporal-frontend -n temporal 7233:7233 &
temporal operator namespace create iwontforget
```

### Phase 3: Observability

```bash
# Prometheus + Grafana
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install kube-prometheus prometheus-community/kube-prometheus-stack \
  --namespace observability

# Loki (logs)
helm repo add grafana https://grafana.github.io/helm-charts
helm install loki grafana/loki-stack \
  --namespace observability --set promtail.enabled=true

# Jaeger (tracing)
helm repo add jaegertracing https://jaegertracing.github.io/helm-charts
helm install jaeger jaegertracing/jaeger \
  --namespace observability --set allInOne.enabled=true
```

### Phase 4: Application Services

#### Step 4.1: Create Secrets

```bash
APP_PG_PASS=$(kubectl get secret app-postgresql-credentials \
  -n infrastructure -o jsonpath='{.data.password}' | base64 -d)

kubectl create secret generic app-db-credentials -n iwontforget \
  --from-literal=dsn="postgres://app:${APP_PG_PASS}@app-postgresql-rw.infrastructure.svc:5432/iwontforget"

kubectl create secret generic telegram-credentials -n iwontforget \
  --from-literal=bot-token="YOUR_BOT_TOKEN"

REDIS_PASS=$(kubectl get secret redis -n infrastructure -o jsonpath='{.data.redis-password}' | base64 -d)
kubectl create secret generic redis-credentials -n iwontforget \
  --from-literal=password="${REDIS_PASS}"
```

#### Step 4.2: Deploy Services (in dependency order)

```bash
kubectl apply -f deploy/user-service/ -n iwontforget
kubectl apply -f deploy/wishlist-service/ -n iwontforget
kubectl apply -f deploy/reminder-service/ -n iwontforget
kubectl apply -f deploy/bot-gateway/ -n iwontforget
```

#### Step 4.3: Create Ingress + Set Webhook

```yaml
# deploy/bot-gateway/ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: bot-gateway-ingress
  namespace: iwontforget
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
spec:
  ingressClassName: nginx
  tls:
    - hosts: ["bot.iwontforget.ru"]
      secretName: bot-tls-cert
  rules:
    - host: bot.iwontforget.ru
      http:
        paths:
          - path: /webhook
            pathType: Prefix
            backend:
              service:
                name: bot-gateway
                port:
                  number: 8443
```

```bash
# Set Telegram webhook
curl -X POST "https://api.telegram.org/bot${BOT_TOKEN}/setWebhook" \
  -d "{\"url\": \"https://bot.iwontforget.ru/webhook\"}"
```

### Phase 5: CI/CD

```bash
# Install ArgoCD
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

# Get admin password
kubectl get secret argocd-initial-admin-secret -n argocd \
  -o jsonpath='{.data.password}' | base64 -d
```

---

## 8. Local Development Environment

For local development, use Docker Compose instead of K8s:

```yaml
# docker-compose.yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: iwontforget
      POSTGRES_USER: app
      POSTGRES_PASSWORD: localdev
    ports: ["5432:5432"]
    volumes:
      - pg_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  kafka:
    image: bitnami/kafka:3.7
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: broker,controller
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
    ports: ["9092:9092"]

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    ports: ["8090:8080"]
    depends_on: [kafka]

  temporal-postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: temporal
      POSTGRES_USER: temporal
      POSTGRES_PASSWORD: temporal

  temporal:
    image: temporalio/auto-setup:latest
    environment:
      DB: postgres12
      DB_PORT: 5432
      POSTGRES_USER: temporal
      POSTGRES_PWD: temporal
      POSTGRES_SEEDS: temporal-postgres
    ports: ["7233:7233"]
    depends_on: [temporal-postgres]

  temporal-ui:
    image: temporalio/ui:latest
    environment:
      TEMPORAL_ADDRESS: temporal:7233
    ports: ["8080:8080"]
    depends_on: [temporal]

volumes:
  pg_data:
```

**Local vs Production comparison:**

| Component | Local (Docker Compose) | Production (K8s) |
|-----------|----------------------|-------------------|
| PostgreSQL | Single container | CloudNativePG cluster (primary + replica) |
| Kafka | Single broker, KRaft | Strimzi 3-broker cluster |
| Redis | Single instance | Sentinel HA (master + 2 replicas) |
| Temporal | auto-setup image | Helm chart with separate PG |
| Services | `go run` (Go) / `python -m bot_gateway` (Python) or Docker | K8s Deployments with HPA |
| Ingress | localhost ports | NGINX Ingress + Cloud LB |
| TLS | Not needed | cert-manager + Let's Encrypt |

**Running locally:**
```bash
# Start infrastructure
docker compose up -d

# Run a Go service locally (connects to Docker infra)
cd services/wishlist-service
DB_DSN="postgres://app:localdev@localhost:5432/iwontforget" \
KAFKA_BROKERS="localhost:9092" \
TEMPORAL_HOST="localhost:7233" \
go run cmd/main.go

# Run Bot Gateway locally (Python — connects to Docker infra)
cd services/bot-gateway
TELEGRAM_BOT_TOKEN="your-token" \
TELEGRAM__MODE="longpoll" \
REDIS__ADDR="localhost:6379" \
SERVICES__USER="localhost:50051" \
SERVICES__WISHLIST="localhost:50052" \
python -m bot_gateway
```

---

## 9. Networking and Service Discovery

### 9.1. How K8s Networking Works

```mermaid
graph TB
    subgraph NODE1["Node 1 - 192.168.1.10"]
        subgraph POD_NET1["Pod Network 10.244.1.0/24"]
            P1["bot-gateway-abc<br/>10.244.1.15"]
            P2["user-service-def<br/>10.244.1.16"]
        end
    end

    subgraph NODE2["Node 2 - 192.168.1.11"]
        subgraph POD_NET2["Pod Network 10.244.2.0/24"]
            P3["bot-gateway-xyz<br/>10.244.2.23"]
            P4["wishlist-svc-ghi<br/>10.244.2.24"]
        end
    end

    SVC["Service: bot-gateway<br/>ClusterIP: 10.96.0.15"]
    SVC -.->|iptables DNAT| P1
    SVC -.->|iptables DNAT| P3

    DNS["CoreDNS<br/>bot-gateway.iwontforget.svc -> 10.96.0.15"]
```

**Three network planes:**

| Plane | CIDR Example | Purpose |
|-------|-------------|---------|
| **Node network** | 192.168.1.0/24 | Physical/VM IPs |
| **Pod network** | 10.244.0.0/16 | Every pod gets a unique IP |
| **Service network** | 10.96.0.0/12 | Virtual IPs for Services |

### 9.2. Network Policies

Control which pods can talk to each other:

```yaml
# Only allow iwontforget namespace to access infrastructure
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-app-to-infra
  namespace: infrastructure
spec:
  podSelector: {}
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: iwontforget
        - namespaceSelector:
            matchLabels:
              name: temporal
```

### 9.3. Service Mesh Consideration (Future)

For Phase 7+, consider adding **Istio** or **Linkerd** for:
- Mutual TLS between services (zero-trust networking)
- Advanced traffic management (canary deployments, traffic splitting)
- Automatic retries and circuit breaking at the mesh level
- Fine-grained observability (per-request metrics without code changes)

Not needed for MVP — K8s Services + gRPC interceptors are sufficient.

---

## 10. Secrets and Configuration Management

### 10.1. Secret Types

| Secret | Namespace | Contains |
|--------|-----------|----------|
| `app-postgresql-credentials` | infrastructure | DB username + password |
| `temporal-postgresql-credentials` | temporal | Temporal DB credentials |
| `app-db-credentials` | iwontforget | Full DSN for app services |
| `telegram-credentials` | iwontforget | Bot token |
| `redis-credentials` | iwontforget | Redis password |
| `ai-service-secrets` | iwontforget | LLM API keys |

### 10.2. ConfigMaps for Service Configuration

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: wishlist-service-config
  namespace: iwontforget
data:
  config.yaml: |
    service:
      name: wishlist-service
      port: 50051
    database:
      schema: wishes
      pool_size: 10
    kafka:
      topic: wish.events
      outbox_poll_interval: 500ms
    observability:
      metrics_port: 9090
      health_port: 8080
      log_level: info
```

### 10.3. External Secrets (Production)

For production, use **External Secrets Operator** to sync secrets from a vault:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: telegram-credentials
  namespace: iwontforget
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: yandex-lockbox
    kind: ClusterSecretStore
  target:
    name: telegram-credentials
  data:
    - secretKey: bot-token
      remoteRef:
        key: iwontforget/telegram
        property: bot-token
```

---

## 11. Observability Stack

### 11.1. What Each Tool Does

```mermaid
graph LR
    subgraph APP["Application Pods"]
        SVC["Service Code"]
    end

    SVC -->|Metrics endpoint :9090| PROM["Prometheus<br/>Time-series metrics"]
    SVC -->|Structured logs stdout| LOKI["Loki<br/>Log aggregation"]
    SVC -->|Trace spans via OTLP| JAEGER["Jaeger<br/>Distributed tracing"]

    PROM --> GRAF["Grafana<br/>Dashboards + Alerts"]
    LOKI --> GRAF
    JAEGER --> GRAF
```

| Tool | What It Collects | Query Language | Use Case |
|------|-----------------|----------------|----------|
| **Prometheus** | Numeric metrics (counters, histograms, gauges) | PromQL | "How many requests per second?" "What is p99 latency?" |
| **Loki** | Log lines from all pods | LogQL | "Show me errors from wishlist-service in the last hour" |
| **Jaeger** | Distributed traces (request path across services) | Trace ID lookup | "Why was this request slow? Which service was the bottleneck?" |
| **Grafana** | Nothing — it visualizes data from the above | — | Dashboards, alerts, correlating metrics + logs + traces |

### 11.2. How a Request is Traced

```mermaid
sequenceDiagram
    participant BG as Bot Gateway
    participant US as User Service
    participant WS as Wishlist Service
    participant PG as PostgreSQL

    Note over BG: Generate trace_id: abc123
    BG->>US: gRPC GetUser<br/>trace_id: abc123, span_id: s1
    US->>PG: SQL query<br/>trace_id: abc123, span_id: s2
    PG-->>US: Result
    US-->>BG: User

    BG->>WS: gRPC CreateWish<br/>trace_id: abc123, span_id: s3
    WS->>PG: SQL INSERT<br/>trace_id: abc123, span_id: s4
    PG-->>WS: OK
    WS-->>BG: Wish

    Note over BG: Total: 45ms
    Note over US: GetUser: 12ms
    Note over WS: CreateWish: 28ms
```

In Jaeger UI, you see the full trace as a waterfall diagram — instantly showing which service/query was slow.

### 11.3. Key Dashboards to Build

| Dashboard | Metrics | Purpose |
|-----------|---------|---------|
| **Service Overview** | Request rate, error rate, latency (RED) | Is the service healthy? |
| **Pod Resources** | CPU, memory, restarts | Are we over/under-provisioned? |
| **PostgreSQL** | Connections, query duration, replication lag | Is the DB healthy? |
| **Kafka** | Consumer lag, partition count, throughput | Are events being processed? |
| **Temporal** | Workflow count, activity duration, task queue depth | Are workflows running? |
| **Redis** | Hit rate, memory usage, connected clients | Is caching effective? |

---

## 12. Troubleshooting Playbook

### Pod Not Starting

```bash
# Check pod status
kubectl get pods -n iwontforget
# STATUS: CrashLoopBackOff, ImagePullBackOff, Pending

# Get details
kubectl describe pod <pod-name> -n iwontforget
# Look at Events section at the bottom

# Common issues:
# ImagePullBackOff → wrong image name or missing registry credentials
# CrashLoopBackOff → app crashes on startup (check logs)
# Pending → not enough resources (check node capacity)
# OOMKilled → increase memory limits

# Check logs
kubectl logs <pod-name> -n iwontforget
kubectl logs <pod-name> -n iwontforget --previous  # If crashed
```

### Service Not Reachable

```bash
# Check service exists and has endpoints
kubectl get svc wishlist-service -n iwontforget
kubectl get endpoints wishlist-service -n iwontforget
# If ENDPOINTS is empty → no healthy pods match the selector

# Test connectivity from another pod
kubectl exec -it <any-pod> -n iwontforget -- \
  wget -qO- http://wishlist-service:8080/healthz

# Port-forward for local testing
kubectl port-forward svc/wishlist-service 50051:50051 -n iwontforget
grpcurl -plaintext localhost:50051 list
```

### Database Connection Issues

```bash
# Check PostgreSQL cluster status
kubectl get cluster -n infrastructure
# STATUS should be "Cluster in healthy state"

# Check if service can reach PG
kubectl exec -it <app-pod> -n iwontforget -- \
  pg_isready -h app-postgresql-rw.infrastructure.svc -p 5432

# Check connection count
kubectl exec -it app-postgresql-1 -n infrastructure -- \
  psql -U app -d iwontforget -c "SELECT count(*) FROM pg_stat_activity;"
```

### Kafka Issues

```bash
# Check Kafka cluster
kubectl get kafka -n infrastructure
# READY should be True

# List topics
kubectl exec -it kafka-0 -n infrastructure -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --list

# Check consumer lag
kubectl exec -it kafka-0 -n infrastructure -- \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group <consumer-group-name>
```

### Temporal Workflow Stuck

```bash
# Port-forward Temporal UI
kubectl port-forward svc/temporal-web -n temporal 8080:8080
# Open http://localhost:8080 → find workflow → check history

# Or use CLI
temporal workflow describe --workflow-id <wf-id> --namespace iwontforget
temporal workflow show --workflow-id <wf-id> --namespace iwontforget
```

---

## 13. Learning Path

A recommended order for learning the infrastructure technologies:

### Stage 1: Containers and Local Development

| # | Topic | What to Learn | Resources |
|---|-------|---------------|-----------|
| 1 | **Docker basics** | Images, containers, Dockerfile, volumes | Docker official tutorial |
| 2 | **Docker Compose** | Multi-container apps, networking, volumes | Compose documentation |
| 3 | **Go basics** | Modules, interfaces, goroutines, channels | Tour of Go, Effective Go |
| 4 | **PostgreSQL** | SQL, indexes, transactions, EXPLAIN | PostgreSQL tutorial |

### Stage 2: Communication Patterns

| # | Topic | What to Learn | Resources |
|---|-------|---------------|-----------|
| 5 | **Protocol Buffers** | Message definitions, code generation, Buf | Buf documentation |
| 6 | **gRPC** | Unary calls, interceptors, error handling | gRPC Go quickstart |
| 7 | **Kafka fundamentals** | Topics, partitions, producers, consumers | Kafka: The Definitive Guide |
| 8 | **Transactional Outbox** | Pattern, implementation, relay | Microservices Patterns book |

### Stage 3: Kubernetes

| # | Topic | What to Learn | Resources |
|---|-------|---------------|-----------|
| 9 | **K8s concepts** | Pods, Deployments, Services, Namespaces | Kubernetes in Action book |
| 10 | **kubectl** | Essential commands, debugging | K8s cheat sheet |
| 11 | **Helm** | Charts, values, releases, repos | Helm documentation |
| 12 | **K8s networking** | Services, DNS, Ingress, Network Policies | K8s networking docs |

### Stage 4: Orchestration and Observability

| # | Topic | What to Learn | Resources |
|---|-------|---------------|-----------|
| 13 | **Temporal** | Workflows, activities, signals, timers | Temporal Go SDK docs |
| 14 | **Prometheus + Grafana** | Metrics, PromQL, dashboards, alerts | Prometheus docs |
| 15 | **Distributed tracing** | OpenTelemetry, Jaeger, trace propagation | OpenTelemetry Go docs |
| 16 | **Structured logging** | Zap/slog, log levels, Loki | Go slog documentation |

### Stage 5: Production Operations

| # | Topic | What to Learn | Resources |
|---|-------|---------------|-----------|
| 17 | **CI/CD** | GitHub Actions, Docker build, ArgoCD | ArgoCD documentation |
| 18 | **K8s operators** | CloudNativePG, Strimzi, how operators work | Operator pattern docs |
| 19 | **Security** | RBAC, Network Policies, Secrets, TLS | K8s security best practices |
| 20 | **Load testing** | k6, Vegeta, identifying bottlenecks | k6 documentation |

### Hands-On Exercises

| Exercise | What You Build | Skills Practiced |
|----------|---------------|------------------|
| **Ex 1** | Hello World gRPC service in Docker | Docker, gRPC, Go |
| **Ex 2** | Deploy it to minikube with Service + Ingress | K8s basics |
| **Ex 3** | Add PostgreSQL via CloudNativePG, connect from service | Operators, Secrets |
| **Ex 4** | Add Kafka, implement Transactional Outbox | Kafka, event patterns |
| **Ex 5** | Add Temporal, create a reminder workflow | Temporal workflows |
| **Ex 6** | Add Prometheus metrics + Grafana dashboard | Observability |
| **Ex 7** | Set up GitHub Actions CI + ArgoCD deploy | CI/CD |
| **Ex 8** | Add a second service, make them talk via gRPC | Service-to-service |