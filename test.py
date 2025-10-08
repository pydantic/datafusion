from dataclasses import dataclass

# Framework code
@dataclass
class Prompt:
    label: str
    prompt: str

    def __str__(self) -> str:
        return self.prompt

class PromptManager:
    def __init__(self) -> None:
        self.prompts = {}

    def register_prompt(self, prompt: Prompt) -> None:
        self.prompts[prompt.label] = prompt

    def update_prompt(self, label: str, new_prompt: str) -> None:
        self.prompts[label].prompt = new_prompt


class AsyncPromptManager:
    def __init__(self) -> None:
        self.prompts = {}

    def register_prompt(self, prompt: Prompt) -> None:
        self.prompts[prompt.label] = prompt
    
    async def run(self) -> None:
        # In a loop listen for updates from logfire, etc.
        raise NotImplementedError

# User code
GREETING = Prompt(label="greeting", prompt="Hello, World!")

def greet() -> None:
    print(GREETING)


async def main() -> None:
    pm = AsyncPromptManager()
    pm.register_prompt(GREETING)
    with anyio.create_task_group() as tg:
        tg.start_soon(pm.run)
    # Start app
    greet()
