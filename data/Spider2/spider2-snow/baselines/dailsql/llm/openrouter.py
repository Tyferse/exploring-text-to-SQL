import json.decoder
import os
import time

import openai

from utils.enums import LLM

# os.environ["http_proxy"]="http://127.0.0.1:61081"
# os.environ["https_proxy"]="http://127.0.0.1:61081"


def init_client(OPENROUTER_API_KEY):
    return openai.OpenAI(
        base_url="https://api.upstage.ai/v1",
        api_key=OPENROUTER_API_KEY,
    )


def ask_chat(client: openai.OpenAI, model: str, messages: list[dict],
             n: int = 1, think: bool = False) -> dict:
    print('>>>model:', model)
    if model == LLM.GPT_OSS_20B:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            n=n,
            extra_body={"reasoning": {"enabled": think}}
        )
    else:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            n=n,
        )
        
    return dict(
        response=[response["choices"][i]["message"]["content"]
                  for i in range(len(response["choices"]))],
        **response["usage"]
    )


def ask_llm(client: openai.OpenAI, model: str, batch: list, n: int = 1,
            think: bool = False):
    try:
        # batch size must be 1
        assert len(batch) == 1, "batch must be 1 in this mode"
        
        messages = [{"role": "user", "content": batch[0]}]
        response = ask_chat(client, model, messages, n, think)
        response['response'] = [response['response']]  # hard-code for batch_size=1
    except (openai.RateLimitError, json.decoder.JSONDecodeError, Exception) as e:
        print(f"Error occurred: {e}")
        # Return hard-coded response
        response = {"total_tokens": 0, "response": [["SELECT" for _ in range(n)]]}
    
    return response
