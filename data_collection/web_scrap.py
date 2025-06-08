import pandas as pd 
import numpy as np
import asyncio
from playwright.async_api import async_playwright

async def extract_meta(url, meta='keywords'):
    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        pagina = await navegador.new_page()
        await pagina.goto(url)
        keywords = await pagina.locator(f'meta[name="{meta}"]').get_attribute('content')
        await navegador.close()
        return keywords
