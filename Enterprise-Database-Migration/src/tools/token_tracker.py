"""
Token Tracker - Tracks LLM token usage across all agents.
"""

from dataclasses import dataclass, field
from typing import Dict, List
import json
from pathlib import Path


@dataclass
class TokenUsage:
    """Token usage for a single LLM call."""
    agent_name: str
    model_name: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


@dataclass
class TokenTracker:
    """Global token usage tracker."""
    usage_records: List[TokenUsage] = field(default_factory=list)
    
    def add_usage(self, agent_name: str, model_name: str, prompt_tokens: int = 0, 
                  completion_tokens: int = 0, total_tokens: int = 0):
        """Record token usage from an LLM call."""
        usage = TokenUsage(
            agent_name=agent_name,
            model_name=model_name,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens or (prompt_tokens + completion_tokens),
        )
        self.usage_records.append(usage)
    
    def get_total_tokens(self) -> int:
        """Get total tokens used across all agents."""
        return sum(u.total_tokens for u in self.usage_records)
    
    def get_usage_by_agent(self) -> Dict[str, int]:
        """Get token usage grouped by agent."""
        usage_by_agent = {}
        for u in self.usage_records:
            if u.agent_name not in usage_by_agent:
                usage_by_agent[u.agent_name] = 0
            usage_by_agent[u.agent_name] += u.total_tokens
        return usage_by_agent
    
    def get_usage_by_model(self) -> Dict[str, int]:
        """Get token usage grouped by model."""
        usage_by_model = {}
        for u in self.usage_records:
            if u.model_name not in usage_by_model:
                usage_by_model[u.model_name] = 0
            usage_by_model[u.model_name] += u.total_tokens
        return usage_by_model
    
    def get_call_count(self) -> int:
        """Get total number of LLM calls."""
        return len(self.usage_records)
    
    def print_summary(self):
        """Print a formatted summary of token usage."""
        print("\n" + "=" * 60)
        print("📊 TOKEN USAGE SUMMARY")
        print("=" * 60)
        
        # Total tokens
        print(f"\n🔢 Total Tokens Used: {self.get_total_tokens():,}")
        print(f"📞 Total LLM Calls: {self.get_call_count()}")
        
        # By model
        print("\n📦 Usage by Model:")
        usage_by_model = self.get_usage_by_model()
        for model, tokens in sorted(usage_by_model.items(), key=lambda x: -x[1]):
            print(f"   • {model}: {tokens:,} tokens")
        
        # By agent
        print("\n🤖 Usage by Agent:")
        usage_by_agent = self.get_usage_by_agent()
        for agent, tokens in sorted(usage_by_agent.items(), key=lambda x: -x[1]):
            print(f"   • {agent}: {tokens:,} tokens")
        
        print("=" * 60 + "\n")
    
    def save_to_file(self, filepath: Path):
        """Save usage data to JSON file."""
        data = {
            "total_tokens": self.get_total_tokens(),
            "total_calls": self.get_call_count(),
            "by_model": self.get_usage_by_model(),
            "by_agent": self.get_usage_by_agent(),
            "records": [
                {
                    "agent": u.agent_name,
                    "model": u.model_name,
                    "prompt_tokens": u.prompt_tokens,
                    "completion_tokens": u.completion_tokens,
                    "total_tokens": u.total_tokens,
                }
                for u in self.usage_records
            ]
        }
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)


# Global singleton instance
_token_tracker: TokenTracker | None = None


def get_token_tracker() -> TokenTracker:
    """Get the global token tracker instance."""
    global _token_tracker
    if _token_tracker is None:
        _token_tracker = TokenTracker()
    return _token_tracker


def reset_token_tracker():
    """Reset the global token tracker."""
    global _token_tracker
    _token_tracker = TokenTracker()


# Model reference for documentation
MODEL_REFERENCE = """
┏━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Agent                 ┃ Model                              ┃
┡━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Introspection Agent   │ LLM_MODEL_FAST (llama-3.3-70b)     │
│ Dependency Agent      │ LLM_MODEL_FAST (llama-3.3-70b)     │
│ Schema Agent          │ LLM_MODEL_COMPLEX (gpt-oss-120b)   │
│ Logic Agent           │ LLM_MODEL_COMPLEX (gpt-oss-120b)   │
│ Sandbox Agent         │ LLM_MODEL_FAST (llama-3.3-70b)     │
│ Error Fixer Agent     │ LLM_MODEL_COMPLEX (gpt-oss-120b)   │
│ Validation Agent      │ LLM_MODEL_FAST (llama-3.3-70b)     │
│ Reporting Agent       │ LLM_MODEL_COMPLEX (gpt-oss-120b)   │
└───────────────────────┴────────────────────────────────────┘
"""


def print_model_reference():
    """Print the model reference table."""
    print("\n🤖 MODEL REFERENCE (from .env)")
    print(MODEL_REFERENCE)
