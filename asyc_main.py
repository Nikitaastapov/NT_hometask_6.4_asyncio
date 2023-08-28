import asyncio
import aiohttp
import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import Column, Integer, String
import time

PG_DSN = 'postgresql+asyncpg://nikita:1234@127.0.0.1:5431/swapi_people_db'
Base = declarative_base()
engine = create_async_engine(PG_DSN)
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

#DB
class SwapiPeople(Base):
    __tablename__ = 'swapi_people'
    id = Column(Integer, primary_key=True, autoincrement=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)

async def download_links(links_list, type, client_session):
    #print('download_links start')
    res_lst = []
    for link in links_list:
        #print(link)
        async with client_session.get(link) as response:
            json_data = await response.json(content_type=None)
            title = json_data.get(type)
            res_lst.append(title)
    res = ', '.join(res_lst)
    #print(res)
    return res
    #print('download_links finish')

async def paste_to_db(people_list):
    #print('paste_to_db start')
    #print(people_list)
    for result in people_list:
        if result == None:
            pass
        else:
            async with Session() as session:
                session.add(SwapiPeople(**result))
                await session.commit()
    #print('paste_to_db finish')


async def get_people(people_id, client_session):
    #print('get_people start')
    async with client_session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        #print(f'{people_id}')
        character = {}
        json_data = await response.json(content_type=None)
        if json_data.get('detail') == "Not found":
            return None
        else:
            film_links = json_data.get('films', [])
            homeworld_links = [json_data.get('homeworld', [])]
            species_link = json_data.get('species', [])
            starships_link = json_data.get('starships', [])
            vehicles_links = json_data.get('vehicles', [])

            films_coro = download_links(film_links, 'title', client_session)
            homeworld_coro = download_links(homeworld_links, 'name', client_session)
            species_coro = download_links(species_link, 'name', client_session)
            starships_coro = download_links(starships_link, 'name', client_session)
            vehicles_coro = download_links(vehicles_links, 'name', client_session)

            fields = await asyncio.gather(films_coro, homeworld_coro, species_coro, starships_coro, vehicles_coro)
            films, homeworld, species, starships, vehicles = fields
            #fields = await asyncio.gather(films_coro)
            #films = fields

            character['birth_year'] = json_data['birth_year']
            character['eye_color'] = json_data['eye_color']
            character['films'] = films
            character['gender'] = json_data['gender']
            character['hair_color'] = json_data['hair_color']
            character['height'] = json_data['height']
            character['homeworld'] = homeworld
            character['mass'] = json_data['mass']
            character['name'] = json_data['name']
            character['skin_color'] = json_data['skin_color']
            character['species'] = species
            character['starships'] = starships
            character['vehicles'] = vehicles
            print(character)
            return character
        #print('get_people finish')

async def main():
    start = -9
    finish = 1
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)
    while start < 90:
        if start == 80:
            finish = 84
        else:
            start+=10
            finish+=10
        async with aiohttp.ClientSession() as client_session:
            coros = [get_people(i, client_session) for i in range(start, finish)]
            results = await asyncio.gather(*coros)
        coro_db = paste_to_db(people_list=results)
        asyncio.create_task(coro_db)

        all_tasks = asyncio.all_tasks()
        all_tasks = all_tasks - {asyncio.current_task()}
        await asyncio.gather(*all_tasks)

#start = datetime.datetime.now()
asyncio.run(main())
#print(datetime.datetime.now() - start)
