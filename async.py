import requests
import asyncio
import aiohttp
from aiohttp import ClientSession
from more_itertools import chunked

from models import engine, Session, Base, SwapiDB


CHUNK_SIZE = 10

def count_chars(link):
    response = requests.get(link)
    response_json = response.json()
    return int(response_json['count'])




async def make_id(link):  #Эту функцию наверное можно было не писать, но я уже устал и не могу придумать красивый код (￣ρ￣)..zzZZ
    cut = link.split('/')
    return int(cut[-2])


async def get_people(people_id):
    session = aiohttp.ClientSession()
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        response_json = await response.json()
        await session.close()
        return response_json


async def make_list_films(list):
    session = aiohttp.ClientSession()
    titles = ''
    if list != '':
        for i in range(0, list.index(list[-1])):
            async with session.get(list[i]) as response:
                response_json = await response.json()
                if titles == '':
                    titles = titles + response_json['title']
                else:
                    titles = titles + ',' + response_json['title']
        await session.close()
        return titles

    else:
        await session.close()
        return 'n/a'

async def make_species_or_starship_or_vehicle_list(list):
    session = aiohttp.ClientSession()
    names = ''
    if list != '':
        for i in range(0, len(list)):
            async with session.get(list[i]) as response:
                response_json = await response.json()
                if names == '':
                    names = names + response_json['name']
                else:
                    names = names + ',' + response_json['name']
        await session.close()
        return names

    else:
        await session.close()
        return 'n/a'




async def paste_to_db(results):
    swapi_people = [SwapiDB(id=await make_id(item['url']),
                            birth_year=item['birth_year'],
                            eye_color=item['eye_color'],
                            films=await make_list_films(item['films']),
                            gender=item['gender'],
                            hair_color=item['hair_color'],
                            height=item['height'],
                            homeworld=item['homeworld'],
                            mass=item['mass'],
                            name=item['name'],
                            skin_color=item['skin_color'],
                            species=await make_species_or_starship_or_vehicle_list(item['species']),
                            starships=await make_species_or_starship_or_vehicle_list(item['starships']),
                            vehicles=await make_species_or_starship_or_vehicle_list(item['vehicles'])) for item in results]

    async with Session() as session:
        session.add_all(swapi_people)
        await session.commit()
        await session.close()



async def main():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    link = 'http://swapi.dev/api/people/'
    session = aiohttp.ClientSession()
    coros = (get_people(i) for i in range(1, count_chars(link) + 1))
    for coros_chunk in chunked(coros, CHUNK_SIZE):
        result = await asyncio.gather(*coros_chunk)
        paste_to_db_task = asyncio.create_task(paste_to_db(result))

    await paste_to_db_task
    await session.close()


asyncio.run(main())