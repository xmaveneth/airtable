from exa_py import Exa
from dotenv import load_dotenv

import os

# Use .env to store your API key or paste it directly into the code
load_dotenv()
exa = Exa(os.getenv('EXA_API_KEY'))

result = exa.search_and_contents(
  "Football club Barcelona",
  type="auto",
  text=True,
)

print(result)