import asyncio
import csv
from typing import List
import aiohttp
from pathlib import Path


class TrancoURLLoader:
    def __init__(self):
        self.tranco_url = "https://tranco-list.eu/download/PY7M/50"
        self.data_dir = Path("data")
        self.output_file = self.data_dir / "tranco_urls.txt"
    
    async def download_tranco_list(self) -> List[str]:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.tranco_url) as response:
                if response.status == 200:
                    content = await response.text()
                    return self.parse_csv_content(content)
                else:
                    raise Exception(f"Failed to download Tranco list: {response.status}")
    
    def parse_csv_content(self, content: str) -> List[str]:
        urls = []
        lines = content.strip().split('\n')
        
        for line in lines:
            if line and ',' in line:
                rank, domain = line.split(',', 1)
                https_url = f"https://{domain.strip()}"
                urls.append(https_url)
        
        return urls
    
    async def generate_url_file(self) -> str:
        self.data_dir.mkdir(exist_ok=True)
        
        urls = await self.download_tranco_list()
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            for url in urls:
                f.write(f"{url}\n")
        
        print(f"Generated {len(urls)} URLs in {self.output_file}")
        return str(self.output_file)
    
    def load_urls_from_file(self) -> List[str]:
        if not self.output_file.exists():
            raise FileNotFoundError(f"URL file not found: {self.output_file}")
        
        with open(self.output_file, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]


async def get_tranco_urls() -> List[str]:
    loader = TrancoURLLoader()
    
    if not loader.output_file.exists():
        await loader.generate_url_file()
    
    return loader.load_urls_from_file()