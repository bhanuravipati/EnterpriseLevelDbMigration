# Data Migration Flow

## Hybrid Streaming Batch Approach

```mermaid
flowchart TB
    subgraph Phase1["Phase 1: Prepare"]
        A[Disable FKs] --> B[Order tables by dependencies]
    end
    
    subgraph Phase2["Phase 2: Stream Data"]
        C[For each table in order]
        C --> D[Fetch batch of 1000 rows]
        D --> E[Transform types]
        E --> F[Bulk INSERT]
        F --> |more rows?| D
        F --> |table done| G[Next table]
    end
    
    subgraph Phase3["Phase 3: Finalize"]
        H[Re-enable FKs] --> I[Reset sequences/auto-increments]
        I --> J[Validate row counts]
    end
    
    Phase1 --> Phase2 --> Phase3
```

## How to View This Diagram

1. **VS Code**: Install "Markdown Preview Mermaid Support" extension
2. **GitHub**: Push to GitHub and view there
3. **Online**: Copy the mermaid code to [mermaid.live](https://mermaid.live)

## Summary

| Phase | Actions |
|-------|---------|
| **Prepare** | Disable FK constraints, get table order |
| **Stream Data** | For each table: fetch 1000 rows → transform → bulk insert |
| **Finalize** | Re-enable FKs, reset sequences, validate counts |