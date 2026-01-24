# src/services/llm.py
import requests
import json
import os

# 你之前的设置，为了避免 requests 被系统代理干扰
os.environ["NO_PROXY"] = "*"


class LLM:
    """
    Minimal LLM interface for querying a local or remote chat model endpoint.
    Supports Ollama / OpenAI-like formats.
    """

    def __init__(
        self,
        api_key: str = "",
        llm_url: str = "http://localhost:11434/api/chat",
        model_name: str = "qwen3:8b",
        format: str = "ollama",
        remove_think: bool = True,
        temperature: float | None = None,
        proxy_url: str = None,  # 如 "http://127.0.0.1:7897"
    ):
        self.api_key = api_key
        self.llm_url = llm_url
        self.model_name = model_name
        self.format = format  # "ollama" or "openai"
        self.remove_think_enabled = remove_think
        self.temperature = temperature
        if proxy_url:
            self.proxies = {
                "http": proxy_url,
                "https": proxy_url,
            }
        else:
            self.proxies = None

    def remove_think(self, text: str) -> str:
        """Remove <think>...</think> sections from model output."""
        start_tag, end_tag = "<think>", "</think>"
        start = text.find(start_tag)
        end = text.find(end_tag, start + len(start_tag)) if start != -1 else -1
        if start != -1 and end != -1:
            text = text[:start] + text[end + len(end_tag):]
        return text.strip()

    def query(self, prompt: str, system_prompt: str = "", verbose: bool = False) -> str:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        # ====== 根据 format 构造 payload ======
        if self.format == "openai_completion":
            full_prompt = prompt
            if system_prompt:
                full_prompt = system_prompt.strip() + "\n\n" + prompt

            payload = {
                "model": self.model_name,
                "prompt": full_prompt,
            }
            if self.temperature is not None:
                payload["temperature"] = self.temperature

        else:
            # chat / ollama / openai-chat
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt + " /no_think"},
            ]
            payload = {
                "model": self.model_name,
                "messages": messages,
                "stream": False,
            }
            if self.temperature is not None:
                payload["temperature"] = self.temperature

        # ====== 发送请求 ======
        response = requests.post(
            self.llm_url,
            headers=headers,
            json=payload,
            proxies=self.proxies,
        )
        response.raise_for_status()
        data = response.json()

        # ====== 解析返回 ======
        if self.format == "openai_completion":
            text = data["choices"][0]["text"]

        elif self.format == "ollama":
            text = data.get("message", {}).get("content", "")

        elif self.format in ["openai", "qwen"]:
            text = data["choices"][0]["message"]["content"]

        else:
            text = str(data)

        if self.remove_think_enabled:
            text = self.remove_think(text)

        if verbose:
            print(f"\n[Prompt]\n{prompt}\n\n[Response]\n{text}\n")

        return text
