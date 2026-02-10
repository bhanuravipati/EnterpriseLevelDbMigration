"""
Base Agent - Foundation class for all migration agents.
Provides LLM integration, tool binding, and common utilities.
"""

import os
from typing import Any, Sequence

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.tools import BaseTool
from langchain_core.callbacks import BaseCallbackHandler
from langchain_groq import ChatGroq

from src.config import get_settings, LLMConfig
from src.tools.token_tracker import get_token_tracker
from src.tools.api_key_manager import get_api_key_manager


def get_langsmith_callbacks() -> list[BaseCallbackHandler]:
    """Get LangSmith callbacks if configured."""
    callbacks = []
    
    # LangSmith is auto-enabled via environment variables
    # LANGCHAIN_TRACING_V2=true
    # LANGCHAIN_API_KEY=your_key
    # LANGCHAIN_PROJECT=your_project
    
    if os.getenv("LANGCHAIN_TRACING_V2", "").lower() == "true":
        print("📊 LangSmith tracing enabled")
    
    return callbacks


class BaseAgent:
    """
    Base class for all migration agents.
    Provides common functionality for LLM interaction and tool usage.
    """
    
    def __init__(
        self,
        name: str,
        description: str,
        use_complex_model: bool = False,
        tools: list[BaseTool] | None = None,
        system_prompt: str | None = None,
    ):
        self.name = name
        self.description = description
        self.tools = tools or []
        self.system_prompt = system_prompt or self._default_system_prompt()
        
        settings = get_settings()
        self.llm_config = settings.llm
        self.use_complex_model = use_complex_model
        
        self._llm: ChatGroq | None = None
        self._llm_with_tools: ChatGroq | None = None
    
    def _default_system_prompt(self) -> str:
        """Default system prompt for the agent."""
        return f"""You are {self.name}, an AI agent specialized in database migration.
Your role: {self.description}

Guidelines:
- Be precise and accurate in your analysis
- Always explain your reasoning
- If you encounter an error, provide clear details for debugging
- Use the provided tools to accomplish your tasks
- Return structured data when possible
"""
    
    @property
    def model_name(self) -> str:
        """Get the appropriate model name based on complexity."""
        if self.use_complex_model:
            return self.llm_config.llm_model_complex
        return self.llm_config.llm_model_fast
    
    @property
    def llm(self) -> ChatGroq:
        """Get or create the LLM instance using current API key."""
        if self._llm is None:
            self._create_llm()
        return self._llm
    
    def _create_llm(self):
        """Create LLM instance with current API key from manager."""
        key_manager = get_api_key_manager()
        self._llm = ChatGroq(
            api_key=key_manager.current_key,
            model=self.model_name,
            temperature=self.llm_config.temperature,
            max_tokens=self.llm_config.max_tokens,
        )
        # Reset tools binding when LLM changes
        self._llm_with_tools = None
    
    def _rotate_api_key_and_retry(self) -> bool:
        """Rotate to next API key and recreate LLM. Returns True if successful."""
        key_manager = get_api_key_manager()
        if key_manager.rotate_key("rate_limited"):
            self._create_llm()
            return True
        return False
    
    @property
    def llm_with_tools(self) -> ChatGroq:
        """Get LLM with tools bound."""
        if self._llm_with_tools is None:
            if self.tools:
                self._llm_with_tools = self.llm.bind_tools(self.tools)
            else:
                self._llm_with_tools = self.llm
        return self._llm_with_tools
    
    def invoke(self, messages: list[BaseMessage]) -> BaseMessage:
        """Invoke the LLM with messages and track token usage."""
        full_messages = [SystemMessage(content=self.system_prompt)] + messages
        response = self.llm_with_tools.invoke(full_messages)
        
        # Track token usage from response metadata
        try:
            if hasattr(response, 'response_metadata'):
                metadata = response.response_metadata
                usage = metadata.get('token_usage', {}) or metadata.get('usage', {})
                if usage:
                    tracker = get_token_tracker()
                    tracker.add_usage(
                        agent_name=self.name,
                        model_name=self.model_name,
                        prompt_tokens=usage.get('prompt_tokens', 0),
                        completion_tokens=usage.get('completion_tokens', 0),
                        total_tokens=usage.get('total_tokens', 0),
                    )
        except Exception:
            pass  # Don't fail on tracking errors
        
        return response
    
    def invoke_with_retry(
        self, 
        messages: list[BaseMessage], 
        max_retries: int | None = None
    ) -> BaseMessage:
        """Invoke the LLM with automatic retries and API key rotation on rate limits."""
        max_retries = max_retries or self.llm_config.max_retries
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return self.invoke(messages)
            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                
                # Check if this is a rate limit error
                is_rate_limit = any(x in error_str for x in [
                    "rate_limit", "rate limit", "429", "too many requests",
                    "quota exceeded", "rate-limit"
                ])
                
                if is_rate_limit:
                    self.log(f"Rate limited on attempt {attempt + 1}, rotating API key...", "warning")
                    if self._rotate_api_key_and_retry():
                        # Successfully rotated, retry without adding error context
                        continue
                    else:
                        # No more keys available
                        self.log("All API keys exhausted!", "error")
                        raise last_error
                
                # For non-rate-limit errors, add context and retry
                if attempt < max_retries - 1:
                    error_msg = HumanMessage(
                        content=f"Previous attempt failed with error: {str(e)}. Please try again."
                    )
                    messages = messages + [error_msg]
        
        raise last_error or Exception("Max retries exceeded")
    
    def create_message(self, content: str) -> HumanMessage:
        """Create a human message."""
        return HumanMessage(content=content)
    
    def extract_text_content(self, message: BaseMessage) -> str:
        """Extract text content from a message."""
        if isinstance(message.content, str):
            return message.content
        elif isinstance(message.content, list):
            # Handle multimodal content
            text_parts = [
                part.get("text", "") for part in message.content 
                if isinstance(part, dict) and "text" in part
            ]
            return "\n".join(text_parts)
        return str(message.content)
    
    def log(self, message: str, level: str = "info"):
        """Log a message (placeholder for proper logging)."""
        prefix = {
            "info": "ℹ️",
            "success": "✅",
            "warning": "⚠️",
            "error": "❌",
        }.get(level, "•")
        print(f"{prefix} [{self.name}] {message}")


class AgentResponse:
    """Structured response from an agent."""
    
    def __init__(
        self,
        success: bool,
        message: str,
        data: Any = None,
        errors: list[str] | None = None,
        artifacts_created: list[str] | None = None,
    ):
        self.success = success
        self.message = message
        self.data = data
        self.errors = errors or []
        self.artifacts_created = artifacts_created or []
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "data": self.data,
            "errors": self.errors,
            "artifacts_created": self.artifacts_created,
        }
