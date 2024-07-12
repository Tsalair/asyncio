import requests
import asyncio
import aiohttp

import datetime
from more_itertools import chunked
from models import init_db, SwapiPeople, Session, engine

MAX_CHUNK = 10


async def get_person(client, person_id):
    
    http_response = await client.get(f'https://swapi.dev/api/people/{person_id}/')
    if http_response.status == 404:
        return None
    json_result = await http_response.json()
    return json_result 

async def get_person_any_lists(client, any_list, any_key):
    person_any_list = []
    for link in any_list:
        http_response = await client.get(f'{link}/')
        json_result = await http_response.json()
        any_key_value = json_result[any_key]
        person_any_list.append(any_key_value)
    person_any_string = ', '.join(person_any_list)
    return person_any_string

async def insert_to_db(client, list_of_jsons):

    persons = [SwapiPeople(
        name=json_item['name'], 
        birth_year=json_item['birth_year'],
        eye_color=json_item['eye_color'],
        gender=json_item['gender'],
        hair_color=json_item['hair_color'],
        height=json_item['height'],
        homeworld=json_item['homeworld'],
        mass=json_item['mass'],
        skin_color=json_item['skin_color'],
        films=await get_person_any_lists(client, json_item['films'], 'title'),
        species=await get_person_any_lists(client, json_item['species'], 'name'),
        starships=await get_person_any_lists(client, json_item['starships'], 'name'),
        vehicles=await get_person_any_lists(client, json_item['vehicles'], 'name')
        ) for json_item in list_of_jsons if json_item is not None]
    
    async with Session() as session:
        session.add_all(persons)
        await session.commit()


async def main():
    await init_db()
    async with aiohttp.ClientSession() as client:
        
        for chunk in chunked(range(1,84), MAX_CHUNK):
            coros = [get_person(client, person_id) for person_id in chunk]
            result = await asyncio.gather(*coros)
            asyncio.create_task(insert_to_db(client, result))          
        tasks_set = asyncio.all_tasks() - {asyncio.current_task()}       
        await asyncio.gather(*tasks_set)   
    await engine.dispose()


if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)