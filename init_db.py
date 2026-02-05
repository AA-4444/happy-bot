import asyncio
from db import init_db, seed_welcome

async def main():
	await init_db()
	await seed_welcome()
	print("DB READY")
	
	

asyncio.run(main())
