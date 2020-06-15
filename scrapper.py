import os
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.collection import ReturnDocument

from pyppeteer import launch
from urllib import parse
import tldextract
from bs4 import BeautifulSoup
import zlib

import time
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()


def now_time():
    return str( int( time.time() * 1000 ) )


class Scrapper:
    def __init__( self ):
        self.__init_variables()  # load & initiate variables
        self.__init_actions()  # run initiate action
        
        asyncio.get_event_loop().run_until_complete( self.__run_main() )  # init main actions
    
    def __init_variables( self ):
        self.__browser_instance_limit = int( os.getenv( 'INITIAL_BROWSER_LIMIT' ) )
        self.__page_instance_limit = int( os.getenv( 'INITIAL_PAGE_LIMIT_FOR_EACH_BROWSER' ) )  # must start 2 or above
        self.__urls_are_set_into_page_instances = False
        
        self.__browser_instances = { }
    
    def __init_actions( self ):
        self.__connect_db()
        
        # get current running instance id
        asyncio.get_event_loop().run_until_complete( self.__get_process_instance_id() )
    
    def __connect_db( self ):
        self.__db_connection = AsyncIOMotorClient( os.getenv( 'MONGODB_CONNECTION' ) )
        
        self.__db = self.__db_connection.amz_watch
        
        self.__process_instances_collection = self.__db.system_process_instances
        self.__browser_instances_collection = self.__db.system_browser_instances
        self.__page_instances_collection = self.__db.system_page_instances
        self.__users_domains = self.__db.users_domains
        self.__domains = self.__db.domains
        self.__pages = self.__db.pages
        self.__pages_meta = self.__db.pages_meta
        self.__pages_inbound_links = self.__db.pages_inbound_links
        self.__pages_outbound_links = self.__db.pages_outbound_links
        self.__amazon_products = self.__db.amazon_products
        self.__amazon_products_meta = self.__db.amazon_products_meta
        self.__amazon_products_in_pages = self.__db.amazon_products_in_pages
        self.__links_in_guest_posts = self.__db.links_in_guest_posts
    
    async def __get_process_instance_id( self ):
        # save instance info when script run
        temp_process_instance_id = await self.__process_instances_collection.insert_one( {
            'browser_instance_limit': self.__browser_instance_limit,
            'page_instance_limit'   : self.__page_instance_limit,
            'process_started_at'    : datetime.utcnow(),
            'process_updated_at'    : '',
            'process_closed_at'     : ''
        } )
        
        self.__process_instance_id = str( temp_process_instance_id.inserted_id )
        
        print( 'Process instance id ----> ' + self.__process_instance_id )
    
    async def __run_main( self ):
        print( 'Starting...' )
        
        await asyncio.gather(
            # self.__get_instances_info(),
            # self.__update_instances_info(),
            self.__handle_instances(),
            
            self.__run_scraper()
        )
    
    async def __get_instances_info( self ):
        # get instance infos
        pass
    
    async def __update_instances_info( self ):
        # run forever for get instance change
        pass
    
    async def __handle_instances( self ):
        # handle all instance from here
        await asyncio.gather(
            self.__handle_process_instances(),
            self.__handle_browser_instances(),
            self.__handle_page_instances()
        )
    
    async def __handle_process_instances( self ):
        # handle process instance
        # if for close this instance then close it
        await asyncio.gather(
            self.__close_process_instance()
        )
    
    async def __close_process_instance( self ):
        # handle process close & its related others
        pass
    
    async def __handle_browser_instances( self ):
        # call for browser start & close actions
        await asyncio.gather(
            self.__start_browser_instance(),
            self.__close_browser_instance()
        )
    
    async def __start_browser_instance( self ):
        while True:
            # if our browser instance is less then the limit then start
            if len( self.__browser_instances ) < self.__browser_instance_limit:
                temp_browser = await launch( headless=True, args=[ '--no-sandbox' ] )
                # temp_browser = 'temp_browser'
                browser_id = await self.__get_browser_instance_id()
                
                self.__browser_instances[ browser_id ] = {
                    'browser': temp_browser,
                    'pages'  : { }
                }
                
                print( 'Created browser instance -----> ' + browser_id )
            
            await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) )
    
    async def __get_browser_instance_id( self ):
        browser_instance_id = await self.__browser_instances_collection.insert_one( {
            'process_id'        : self.__process_instance_id,
            'browser_started_at': datetime.utcnow(),
            'browser_closed_at' : ''
        } )
        
        return str( browser_instance_id.inserted_id )
    
    async def __close_browser_instance( self ):
        while True:
            # if exceed the limit close browser instance until its equal to the limit
            if len( self.__browser_instances ) > self.__browser_instance_limit:
                await self.__close_a_browser_instance()
            
            await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) )
    
    async def __close_a_browser_instance( self, instance_id=None ):
        # handle closing a browser instance forcefully if id present or close randomly
        pass
    
    async def __update_browser_close_time( self ):
        # update browser instance close info
        pass
    
    async def __handle_page_instances( self ):
        # call for page start & close actions
        await asyncio.gather(
            self.__start_page_instance(),
            self.__close_page_instance()
        )
    
    async def __start_page_instance( self ):
        while True:
            # handle each browsers page start by limit
            for browser in self.__browser_instances:
                # currently we r using same page limit for all browser
                if len( self.__browser_instances[ browser ][ 'pages' ] ) < self.__page_instance_limit:
                    temp_page = await self.__browser_instances[ browser ][ 'browser' ].newPage()
                    # temp_page = 'temp page'
                    page_id = await self.__get_page_instance_id( browser )
                    
                    self.__browser_instances[ browser ][ 'pages' ][ page_id ] = {
                        'page'          : temp_page,
                        'working_status': 'false',
                        'info'          : { }
                    }
                    
                    print( 'Created page instance -----> ' + page_id )
            
            await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) )
    
    async def __get_page_instance_id( self, browser_id ):
        page_instance_id = await self.__page_instances_collection.insert_one( {
            'browser_id'     : browser_id,
            'page_started_at': datetime.utcnow(),
            'page_closed_at' : ''
        } )
        
        return str( page_instance_id.inserted_id )
    
    async def __close_page_instance( self ):
        while True:
            # handle each browsers page close by limit
            for browser in self.__browser_instances:
                # currently we r using same page limit for all browser
                if len( self.__browser_instances[ browser ][ 'pages' ] ) > self.__page_instance_limit:
                    await self.__close_a_page_instance( browser )
            
            await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) )
    
    async def __close_a_page_instance( self, browser_id, page_id=None ):
        # handle randomly close a page by browser or forcefully by page_id for that browser
        # browser_id must needed
        pass
    
    async def __update_page_close_time( self ):
        # update page instance close info
        pass
    
    async def __run_scraper( self ):
        print( 'Starting scrapper working instances' )
        
        await asyncio.gather(
            self.__side_works(),
            self.__do_before_scraping(),
            self.__do_scraping(),
            self.__do_after_scraping(),
            self.__do_side_by_side_works()
        )
    
    async def __side_works( self ):
        # handle side works. its for scrape help
        await asyncio.gather(
            self.__assign_domain_url_to_pages(),
            self.__assign_guest_post_url_to_pages()
        )
        # pass
    
    async def __assign_domain_url_to_pages( self ):
        # it will take domain url & assign to pages collection for scrape if not has in there
        # no need to assign guest domain url
        while True:
            pipeline = [
                {
                    "$lookup": {
                        "from"    : "users_domains",
                        
                        "let"     : {
                            # in let get parent vars
                            "id": {
                                "$toString": "$_id"
                            }
                        },
                        
                        "pipeline": [
                            {
                                "$match": {
                                    # use $$ to use the let var & $ to use lookups collection
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [ "$$id", "$domain_id" ]
                                            },
                                            {
                                                "$or": [
                                                    # if need with status wrap each with $and & check status also
                                                    {
                                                        "$ne": [
                                                            {
                                                                "$type": "$domain_use_for.broken_links_check_service"
                                                            },
                                                            'missing'
                                                        ]
                                                    },
                                                    {
                                                        "$ne": [
                                                            {
                                                                "$type": "$domain_use_for.amazon_products_check_service"
                                                            },
                                                            'missing'
                                                        ]
                                                    },
                                                    {
                                                        "$ne": [
                                                            {
                                                                "$type": "$domain_use_for.pages_speed_check_service"
                                                            },
                                                            'missing'
                                                        ]
                                                    },
                                                ]
                                            }
                                        ]
                                        
                                    }
                                },
                            }
                        ],
                        "as"      : "user_domains"
                    }
                },
                # use unwind if needed. it will generate results by path array
                # if your result is {a: a, b: [c: c, d: d]}
                # then unwind result {a: a, b: {c: c}} & {a: a, b: {d: d}}
                # {
                #     "$unwind": { "path": "$user_domains" }
                # },
                
                # sort should be applied before limit
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$gt": [ {
                                        "$size": "$user_domains"
                                    }, 0 ]
                                }
                            ]
                        }
                    }
                },
                
                # {
                #     "$project": {
                #         # reformat parent result
                #         "user_domains": { '$size': '$user_domains' },
                #     }
                # },
                
                {
                    "$sort": {
                        "updated_at.domain_url_in_page_last_check_at": 1
                    }
                },
                {
                    "$limit": 1
                },
            ]
            
            async for domain in self.__domains.aggregate( pipeline ):
                # print( domain )
                
                # if need use transaction
                await self.__pages.update_one(
                    {
                        "domain_id": str( domain[ '_id' ] ),
                        "url"      : domain[ 'url' ]
                    },
                    {
                        "$setOnInsert": {
                            "domain_id" : str( domain[ '_id' ] ),
                            "url"       : domain[ 'url' ],
                            "updated_at": {
                                "last_scraped_at": "1"
                            }
                        }
                    },
                    upsert=True
                )
                await self.__domains.update_one(
                    {
                        "_id": domain[ '_id' ],
                    },
                    {
                        "$set": {
                            "updated_at": {
                                "domain_url_in_page_last_check_at": now_time()
                            }
                        }
                    },
                    upsert=True
                )
            
            await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) + 10 )
    
    async def __assign_guest_post_url_to_pages( self ):
        while True:
            pipeline = [
                {
                    "$match": {
                        "$expr": {
                            "$or": [
                                {
                                    "$eq": [
                                        {
                                            "$type": "$updated_at"
                                        },
                                        'missing'
                                    ]
                                },
                                {
                                    "$eq": [
                                        {
                                            "$type": "$updated_at.guest_post_url_in_page_last_checked_at"
                                        },
                                        'missing'
                                    ]
                                },
                                {
                                    "$eq": [ "$updated_at.guest_post_url_in_page_last_checked_at", "1" ]
                                }
                            ]
                        }
                    }
                },
                
                {
                    "$lookup": {
                        "from"    : "users_domains",
                        
                        "let"     : {
                            # in let get parent vars
                            "user_domain_id": "$user_domain_id"
                        },
                        
                        "pipeline": [
                            {
                                "$match": {
                                    # use $$ to use the let var & $ to use lookups collection
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [
                                                    "$$user_domain_id",
                                                    {
                                                        "$toString": "$_id"
                                                    }
                                                ]
                                            },
                                        ]
                                    }
                                },
                            }
                        ],
                        "as"      : "user_domain"
                    }
                },
                
                {
                    "$sort": {
                        "updated_at.guest_post_url_in_page_last_checked_at": 1
                    }
                },
                {
                    "$limit": 10
                },
            ]
            
            async for guest_post in self.__links_in_guest_posts.aggregate( pipeline ):
                # print(guest_post)
                service_info = guest_post[ 'user_domain' ][ 0 ][ 'domain_use_for' ][ 'guest_posts_check_service' ]
                guest_domain_id = service_info[ 'guest_domain_id' ]
                guest_post_url = guest_post[ 'guest_post_url' ]
                
                # if need use transaction
                await self.__pages.update_one(
                    {
                        "domain_id": guest_domain_id,
                        "url"      : guest_post_url
                    },
                    {
                        "$setOnInsert": {
                            "domain_id" : guest_domain_id,
                            "url"       : guest_post_url,
                            "updated_at": {
                                "last_scraped_at": "1"
                            }
                        }
                    },
                    upsert=True
                )
                
                await self.__links_in_guest_posts.update_one(
                    {
                        "_id": guest_post[ '_id' ]
                    },
                    {
                        "$set": {
                            "updated_at.guest_post_url_in_page_last_checked_at": now_time()
                        }
                    }
                )
            
            await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) + 5 )
    
    async def __do_before_scraping( self ):
        await asyncio.gather(
            self.__get_and_assign_pages_to_instances()
        )
    
    async def __get_and_assign_pages_to_instances( self ):
        # get pages from collection by limit then assign these into browser pages
        # if all pages status are false then only run this
        while True:
            if not self.__urls_are_set_into_page_instances:
                # we r safe cz if limit changes midway no harm for this block
                get_pages_upto = self.__browser_instance_limit * self.__page_instance_limit
                
                print( 'getting pages & products for scraping' )
                
                pipeline_for_product_pages = [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {
                                        "$lt": [
                                            {
                                                "$sum": [
                                                    {
                                                        "$convert": {
                                                            "input"  : '$updated_at.last_scraped_at',
                                                            "to"     : 'double',
                                                            "onError": 0,
                                                            "onNull" : 0
                                                        }
                                                    },
                                                    3600 * 1000
                                                ]
                                            },
                                            int( time.time() * 1000 )
                                        ]
                                    }
                                ]
                                # handle status & other match
                            }
                        }
                    },
                    {
                        "$sort": {
                            "updated_at.last_scraped_at": 1
                        }
                    },
                    {
                        # limit only 3 so that we are only scrape 3 products at a time
                        "$limit": 3 if get_pages_upto > 3 else 1
                    }
                ]
                
                # its only for if product returns less then 3 then we will use extra slot for pages
                product_found = 0
                
                async for product_page in self.__amazon_products.aggregate( pipeline_for_product_pages ):
                    self.__assign_pages_into_instances( product_page[ 'url' ], 'product_page', product_page )
                    
                    product_found += 1
                    # print(product_page)
                
                # get pages
                if get_pages_upto - product_found != 0:
                    pipeline_for_pages = [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {
                                            "$lt": [
                                                {
                                                    "$sum": [
                                                        {
                                                            "$convert": {
                                                                "input"  : '$updated_at.last_scraped_at',
                                                                "to"     : 'double',
                                                                "onError": 0,
                                                                "onNull" : 0
                                                            }
                                                        },
                                                        3600 * 1000
                                                    ]
                                                },
                                                int( time.time() * 1000 )
                                            ]
                                        }
                                    ]
                                    # handle status & other match
                                }
                            }
                        },
                        {
                            # this sort is used before group
                            "$sort": {
                                "updated_at.last_scraped_at": 1
                            }
                        },
                        {
                            "$group": {
                                "_id" : "$domain_id",
                                # only 1 page from 1 domain is taken currently
                                # if use push group will try to get all pages from a domain
                                # if we get all page use slice. but its not gd
                                "page": {
                                    "$first": "$$ROOT"
                                }
                            }
                        },
                        {
                            # this is for after final sorting
                            "$sort": {
                                "page.updated_at.last_scraped_at": 1
                            }
                        },
                        {
                            "$limit": (get_pages_upto - product_found)
                        }
                    ]
                    
                    async for page in self.__pages.aggregate( pipeline_for_pages ):
                        self.__assign_pages_into_instances( page[ 'page' ][ 'url' ], 'normal_page', page )
                    
                    # print(page)
                
                self.__urls_are_set_into_page_instances = True
            
            await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) )
    
    def __assign_pages_into_instances( self, url, scrape_type, other_info ):
        # assign tem cz __browser_instances can be changed anytime
        temp_browser_instance = self.__browser_instances
        
        for browser in temp_browser_instance:
            for page in temp_browser_instance[ browser ][ 'pages' ]:
                # check if exist main cz these can change midway
                if (
                    temp_browser_instance[ browser ][ 'pages' ][ page ][ 'working_status' ] == 'false'
                    and self.__browser_instances[ browser ]
                    and self.__browser_instances[ browser ][ 'pages' ][ page ]
                ):
                    self.__browser_instances[ browser ][ 'pages' ][ page ][ 'info' ] = {
                        "url"        : url,
                        "scrape_type": scrape_type,
                        "other_info" : other_info
                    }
                    self.__browser_instances[ browser ][ 'pages' ][ page ][ 'working_status' ] = 'pending'
                    
                    # assign to page_instance_urls if track url
                    
                    return  # it will break from parents also
    
    async def __do_scraping( self ):
        await asyncio.gather(
            self.__start_scrapping()
        )
    
    async def __start_scrapping( self ):
        # get all pages and insert into asyncio.gather
        # after all page finish update status so that new pages can be assigned into pages instance
        while True:
            if self.__urls_are_set_into_page_instances:
                temp_browser_instance = self.__browser_instances
                task_list = [ ]
                
                for browser in temp_browser_instance:
                    for page in temp_browser_instance[ browser ][ 'pages' ]:
                        page_instance_data = temp_browser_instance[ browser ][ 'pages' ][ page ]
                        
                        # check if main instances are present
                        if (
                            page_instance_data[ 'working_status' ] == 'pending'
                            and self.__browser_instances[ browser ]
                            and self.__browser_instances[ browser ][ 'pages' ][ page ]
                        ):
                            task_list.append( self.__page_scrapper_task(
                                page_instance_data,
                                page,
                                browser
                            ) )
                
                await asyncio.gather( *task_list )
                
                self.__urls_are_set_into_page_instances = False
            
            await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) )
    
    async def __page_scrapper_task( self, page_instance_data, page_id, browser_id ):
        # handle page scrape
        if self.__check_if_browser_and_page_instance_present( browser_id, page_id ):
            self.__browser_instances[ browser_id ][ 'pages' ][ page_id ][ 'working_status' ] = 'true'
            
            await self.__handle_page_scrape( page_instance_data )
        
        # again check cz upper actions can take time & in that time browser or page may be removed
        if self.__check_if_browser_and_page_instance_present( browser_id, page_id ):
            self.__browser_instances[ browser_id ][ 'pages' ][ page_id ][ 'working_status' ] = 'false'
            self.__browser_instances[ browser_id ][ 'pages' ][ page_id ][ 'info' ] = { }
    
    def __check_if_browser_and_page_instance_present( self, browser_id, page_id, browser_instance=None ):
        if not browser_instance:
            browser_instance = self.__browser_instances
        
        return (
            self.__check_if_browser_instance_present( browser_id, browser_instance )
            and self.__check_if_page_instance_present( page_id, browser_instance[ browser_id ][ 'pages' ] )
        )
    
    def __check_if_browser_instance_present( self, browser_id, browser_instance=None ):
        if not browser_instance:
            browser_instance = self.__browser_instances
        
        return browser_instance[ browser_id ]
    
    def __check_if_page_instance_present( self, page_id, page_instance=None, browser_id=None ):
        if not page_instance:
            page_instance = self.__browser_instances[ browser_id ][ 'pages' ]
        
        return page_instance[ page_id ]
    
    async def __handle_page_scrape( self, page_instance_data ):
        # print( '-------------------------------------------------------' )
        # print( self.__browser_instances )
        # print( page_instance_data )
        # print( '-------------------------------------------------------' )
        
        page_info = page_instance_data[ "info" ]
        page_scrape_type = page_info[ 'scrape_type' ]
        goto_url = page_info[ "url" ]
        page_instance = page_instance_data[ "page" ]
        
        page_content_compressed = zlib.compress( ''.encode(), 5 )
        
        if page_scrape_type == 'normal_page':
            page_id = page_info[ 'other_info' ][ 'page' ][ '_id' ]
            
            print( 'Getting page ----- ' + goto_url + ' -----' )
            
            try:
                page_response = await page_instance.goto( goto_url )
                page_status = page_response.status
                
                if str( page_status ) and str( page_status )[ 0 ] in [ '2', '3' ]:  # check if status code 2xx or 3xx
                    # page_headers = page_response.headers
                    page_content = await page_instance.content()
                    page_content_compressed = zlib.compress( page_content.encode(), 5 )
                
                await self.__update_page_scraped_time( page_id )
                
                await self.__update_page_meta( page_id, page_content_compressed, page_status )
            except:
                print( 'Something went wrong with page ' + goto_url )
                
                await self.__update_page_scraped_time( page_id )
                
                await self.__update_page_meta( page_id, page_content_compressed, 999 )
        else:
            product_page_id = page_info[ 'other_info' ][ '_id' ]
            
            print( 'Getting product page ----- ' + goto_url + ' -----' )
            
            page_content_compressed = zlib.compress( ''.encode(), 5 )
            
            try:
                page_response = await page_instance.goto( goto_url )
                page_status = page_response.status
                
                if str( page_status ) and str( page_status )[ 0 ] in [ '2', '3' ]:  # check if status code 2xx or 3xx :
                    # page_headers = page_response.headers
                    page_content = await page_instance.content()
                    page_content_compressed = zlib.compress( page_content.encode(), 5 )
                
                await self.__update_product_page_scraped_time( product_page_id )
                
                await self.__update_product_page_meta( product_page_id, page_content_compressed, page_status )
            except:
                print( 'Something went wrong with page ' + goto_url )
                
                await self.__update_product_page_scraped_time( product_page_id )
                
                await self.__update_product_page_meta( product_page_id, page_content_compressed, 999 )
        
        await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) )
    
    async def __update_page_scraped_time( self, page_id ):
        await self.__pages.update_one(
            {
                "_id": page_id
            },
            {
                "$set": {
                    "updated_at.last_scraped_at": now_time()
                }
            }
        )
    
    async def __update_page_meta( self, page_id, compressed_content, status ):
        await self.__pages_meta.update_one(
            {
                "page_id": str( page_id )
            },
            {
                "$set": {
                    "page_id"           : str( page_id ),
                    "compressed_content": compressed_content,
                    "page_status"       : status
                }
            },
            upsert=True
        )
    
    async def __update_product_page_scraped_time( self, product_page_id ):
        await self.__amazon_products.update_one(
            {
                "_id": product_page_id
            },
            {
                "$set": {
                    "updated_at.last_scraped_at": now_time()
                }
            }
        )
    
    async def __update_product_page_meta( self, product_page_id, compressed_content, status ):
        await self.__amazon_products_meta.update_one(
            {
                "amazon_product_id": str( product_page_id )
            },
            {
                "$set": {
                    "amazon_product_id" : str( product_page_id ),
                    "compressed_content": compressed_content,
                    "page_status"       : status
                }
            },
            upsert=True
        )
    
    async def __do_after_scraping( self ):
        await asyncio.gather(
            self.__parse_page(),
            self.__parse_product_page(),
            self.__check_links_in_guest_posts(),
        )
    
    async def __parse_page( self ):
        # handle page parsing
        print( 'Page parsing started...' )
        
        while True:
            pipeline = [
                {
                    "$lookup": {
                        "from"    : "pages_meta",
                        
                        "let"     : {
                            # in let get parent vars
                            "id": {
                                "$toString": "$_id"
                            }
                        },
                        
                        "pipeline": [
                            {
                                "$match": {
                                    # use $$ to use the let var & $ to use lookups collection
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [ "$$id", "$page_id" ]
                                            },
                                            {
                                                "$ne": [
                                                    {
                                                        "$type": '$compressed_content'
                                                    },
                                                    'missing'
                                                ]
                                            }
                                        ]
                                        
                                    }
                                },
                            }
                        ],
                        "as"      : "page_meta"
                    }
                },
                
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$lt": [
                                        {
                                            "$sum": [
                                                {
                                                    "$convert": {
                                                        "input"  : '$updated_at.last_parsed_at',
                                                        "to"     : 'double',
                                                        "onError": 0,
                                                        "onNull" : 0
                                                    }
                                                },
                                                3600 * 1000
                                            ]
                                        },
                                        int( time.time() * 1000 )
                                    ]
                                },
                                {
                                    "$gt": [ {
                                        "$size": "$page_meta"
                                    }, 0 ]
                                }
                            ]
                        }
                    }
                },
                
                {
                    "$lookup": {
                        "from"    : "domains",
                        
                        "let"     : {
                            # in let get parent vars
                            "domain_id": "$domain_id"
                        },
                        
                        "pipeline": [
                            {
                                "$match": {
                                    # use $$ to use the let var & $ to use lookups collection
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [ "$$domain_id", {
                                                    '$toString': "$_id"
                                                } ]
                                            }
                                        ]
                                        
                                    }
                                },
                            }
                        ],
                        "as"      : "domain"
                    }
                },
                
                {
                    "$sort": {
                        "updated_at.last_parsed_at": 1
                    }
                },
                {
                    "$limit": 10
                },
            ]
            
            async for page in self.__pages.aggregate( pipeline ):
                print( 'Parsing page ' + page[ 'url' ] )
                
                content = BeautifulSoup( zlib.decompress( page[ 'page_meta' ][ 0 ][ 'compressed_content' ] ),
                                         features='lxml' )
                
                domain_use_for = await self.__get_domain_use_for_from_domain_id( page[ 'domain_id' ] )
                
                inbound_links = [ ]
                outbound_links = [ ]
                
                for raw_link in content.find_all( 'a', href=True ):
                    # for urljoin base must contain scheme
                    # urljoin('some', 'thing')
                    # 'thing'
                    # urljoin('http://some', 'thing')
                    # 'http://some/thing'
                    # urljoin('http://some/more', 'thing')
                    # 'http://some/thing'
                    # urljoin('http://some/more/', 'thing') # just a tad / after 'more'
                    # 'http://some/more/thing'
                    # urljoin('http://some/more/', '/thing')
                    # 'http://some/thin
                    
                    joined_url = parse.urljoin( page[ 'domain' ][ 0 ][ 'url' ], raw_link[ 'href' ] )
                    
                    parsed_page_base = parse.urlparse( page[ 'url' ] )
                    parsed_joined_url = parse.urlparse( joined_url )
                    
                    if parsed_joined_url[ 0 ] in [ 'http', 'https' ] and parsed_joined_url[ 1 ]:
                        final_url = parse.urlunparse( (
                            parsed_joined_url.scheme,
                            parsed_joined_url.netloc,
                            parsed_joined_url.path,
                            '',
                            '',
                            ''
                        ) )
                        
                        if parsed_page_base[ 0 ] == parsed_joined_url[ 0 ] and parsed_page_base[ 1 ] == \
                            parsed_joined_url[ 1 ]:
                            if {
                                'link': final_url
                            } not in inbound_links:
                                inbound_links.append(
                                    {
                                        'link': final_url
                                    }
                                )
                        else:
                            if {
                                'link': final_url
                            } not in outbound_links:
                                outbound_links.append(
                                    {
                                        'link': final_url
                                    }
                                )
                        
                        # print( parse.urlparse( joined_url ) )
                
                if \
                    domain_use_for and \
                        ('broken_links_check_service' in domain_use_for \
                         or 'amazon_products_check_service' in domain_use_for \
                         or 'pages_speed_check_service' in domain_use_for) \
                    :
                    for inbound_link in inbound_links:
                        await self.__pages.update_one(
                            {
                                'domain_id': str( page[ 'domain' ][ 0 ][ '_id' ] ),
                                'url'      : inbound_link[ 'link' ]
                            },
                            {
                                '$setOnInsert': {
                                    'domain_id' : str( page[ 'domain' ][ 0 ][ '_id' ] ),
                                    'url'       : inbound_link[ 'link' ],
                                    'updated_at': {
                                        'last_scraped_at': "2"
                                    }
                                }
                            },
                            upsert=True
                        )
                
                if domain_use_for and 'amazon_products_check_service' in domain_use_for:
                    for outbound_link in outbound_links:
                        if tldextract.extract( outbound_link[ 'link' ] ).domain == 'amazon':
                            parsed_outbound_url = parse.urlparse( outbound_link[ 'link' ] )
                            parsed_query_params = parse.parse_qs( parsed_outbound_url.query )
                            
                            if 'tag' in parsed_query_params:
                                affiliate_id = parsed_query_params[ 'tag' ][ 0 ]
                                
                                if 'affiliate_ids' in domain_use_for[ 'amazon_products_check_service' ]:
                                    affiliate_ids = domain_use_for[ 'amazon_products_check_service' ][ 'affiliate_ids' ]
                                    
                                    if affiliate_id in affiliate_ids:
                                        only_product_url = parse.urlunparse( (
                                            parsed_outbound_url.scheme,
                                            parsed_outbound_url.netloc,
                                            parsed_outbound_url.path,
                                            '',
                                            '',
                                            ''
                                        ) )
                                        
                                        product = await self.__amazon_products.find_one_and_update(
                                            {
                                                'url': only_product_url
                                            },
                                            {
                                                '$setOnInsert': {
                                                    'url': only_product_url
                                                }
                                            },
                                            upsert=True,
                                            return_document=ReturnDocument.AFTER
                                        )
                                        
                                        await self.__amazon_products_in_pages.update_one(
                                            {
                                                'product_id'        : str( product[ '_id' ] ),
                                                'page_id'           : str( page[ '_id' ] ),
                                                'actual_product_url': outbound_link[ 'link' ],
                                                'affiliate_id'      : affiliate_id
                                            },
                                            {
                                                '$setOnInsert': {
                                                    'product_id'        : str( product[ '_id' ] ),
                                                    'page_id'           : str( page[ '_id' ] ),
                                                    'actual_product_url': outbound_link[ 'link' ],
                                                    'affiliate_id'      : affiliate_id
                                                }
                                            },
                                            upsert=True
                                        )
                    
                    prev_actual_product_urls = await self.__get_previous_products_in_pages( str( page[ '_id' ] ) )
                    current_actual_product_urls = [ outbound_link[ 'link' ] for outbound_link in outbound_links ]
                    
                    for_deletes = list( set( prev_actual_product_urls ) - set( current_actual_product_urls ) )
                    
                    for for_delete in for_deletes:
                        await self.__amazon_products_in_pages.delete_one(
                            {
                                'page_id'           : str( page[ '_id' ] ),
                                'actual_product_url': for_delete
                            }
                        )
                        
                        # add inbound links & outbound links
                await self.__insert_inbound_links( str( page[ '_id' ] ),
                                                   [ inbound_link[ 'link' ] for inbound_link in inbound_links ] )
                await self.__insert_outbound_links( str( page[ '_id' ] ),
                                                    [ outbound_link[ 'link' ] for outbound_link in outbound_links ] )
                
                await self.__pages.update_one(
                    {
                        '_id': page[ '_id' ]
                    },
                    {
                        '$set': {
                            'updated_at.last_parsed_at': now_time()
                        }
                    }
                )
            
            await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) )
    
    async def __get_domain_use_for_from_domain_id( self, domain_id ):
        pipeline = [
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {
                                "$eq": [ '$domain_id', domain_id ]
                            }
                            # check status also if project is still running
                        ]
                    }
                }
            },
            
            {
                '$group': {
                    '_id'           : '$domain_id',
                    'domain_use_for': {
                        '$mergeObjects': '$domain_use_for'
                    }
                }
            }
        ]
        
        async for user_domain in self.__users_domains.aggregate( pipeline ):
            return user_domain[ 'domain_use_for' ]
    
    async def __insert_inbound_links( self, page_id, links ):
        prev_links = await self.__get_previous_inbound_links( page_id )
        
        if not prev_links:
            if links:
                await self.__pages_inbound_links.insert_many(
                    [
                        {
                            'page_id': page_id,
                            'url'    : link
                        } for link in links
                    ]
                )
        else:
            for_deletes = list( set( prev_links ) - set( links ) )
            for_inserts = list( set( links ) - set( prev_links ) )
            
            for for_delete in for_deletes:
                await self.__pages_inbound_links.delete_one(
                    {
                        'page_id': page_id,
                        'url'    : for_delete
                    }
                )
            
            if for_inserts:
                await self.__pages_inbound_links.insert_many(
                    [
                        {
                            'page_id': page_id,
                            'url'    : for_insert
                        } for for_insert in for_inserts
                    ]
                )
    
    async def __insert_outbound_links( self, page_id, links ):
        prev_links = await self.__get_previous_outbound_links( page_id )
        
        if not prev_links:
            if links:
                await self.__pages_outbound_links.insert_many(
                    [
                        {
                            'page_id': page_id,
                            'url'    : link
                        } for link in links
                    ]
                )
        else:
            for_deletes = list( set( prev_links ) - set( links ) )
            for_inserts = list( set( links ) - set( prev_links ) )
            
            for for_delete in for_deletes:
                await self.__pages_outbound_links.delete_one(
                    {
                        'page_id': page_id,
                        'url'    : for_delete
                    }
                )
            
            if for_inserts:
                await self.__pages_outbound_links.insert_many(
                    [
                        {
                            'page_id': page_id,
                            'url'    : for_insert
                        } for for_insert in for_inserts
                    ]
                )
    
    async def __get_previous_inbound_links( self, page_id ):
        pipeline = [
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {
                                "$eq": [ "$page_id", page_id ]
                            }
                        ]
                        
                    }
                },
            }
        ]
        
        prev_inbound_links = [ ]
        
        async for inbound_link in self.__pages_inbound_links.aggregate( pipeline ):
            prev_inbound_links.append( inbound_link[ 'url' ] )
        
        return prev_inbound_links
    
    async def __get_previous_outbound_links( self, page_id ):
        pipeline = [
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {
                                "$eq": [ "$page_id", page_id ]
                            }
                        ]
                        
                    }
                },
            }
        ]
        
        prev_outbound_links = [ ]
        
        async for inbound_link in self.__pages_outbound_links.aggregate( pipeline ):
            prev_outbound_links.append( inbound_link[ 'url' ] )
        
        return prev_outbound_links
    
    async def __get_previous_products_in_pages( self, page_id ):
        pipeline = [
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {
                                "$eq": [ "$page_id", page_id ]
                            }
                        ]
                        
                    }
                }
            }
        ]
        
        actual_product_url = [ ]
        
        async for product_in_page in self.__amazon_products_in_pages.aggregate( pipeline ):
            actual_product_url.append( product_in_page[ 'actual_product_url' ] )
        
        return actual_product_url
    
    async def __parse_product_page( self ):
        # handle product page parsing
        print( 'Product page parsing started...' )
        
        while True:
            pipeline = [
                {
                    "$lookup": {
                        "from"    : "amazon_products_meta",
                        
                        "let"     : {
                            # in let get parent vars
                            "id": {
                                "$toString": "$_id"
                            }
                        },
                        
                        "pipeline": [
                            {
                                "$match": {
                                    # use $$ to use the let var & $ to use lookups collection
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [ "$$id", "$amazon_product_id" ]
                                            },
                                            {
                                                "$lt": [
                                                    {
                                                        "$sum": [
                                                            {
                                                                "$convert": {
                                                                    "input"  : '$updated_at.last_parsed_at',
                                                                    "to"     : 'double',
                                                                    "onError": 0,
                                                                    "onNull" : 0
                                                                }
                                                            },
                                                            3600 * 1000
                                                        ]
                                                    },
                                                    int( time.time() * 1000 )
                                                ]
                                            },
                                            {
                                                "$ne": [
                                                    {
                                                        "$type": '$compressed_content'
                                                    },
                                                    'missing'
                                                ]
                                            }
                                        ]
                                        
                                    }
                                },
                            }
                        ],
                        "as"      : "product_page_meta"
                    }
                },
                
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$gt": [ {
                                        "$size": "$product_page_meta"
                                    }, 0 ]
                                }
                            ]
                        }
                    }
                },
                
                {
                    "$sort": {
                        "updated_at.last_parsed_at": 1
                    }
                },
                {
                    "$limit": 10
                },
            ]
            
            async for product_page in self.__amazon_products.aggregate( pipeline ):
                print( 'Parsing page ' + product_page[ 'url' ] )
                
                content = BeautifulSoup(
                    zlib.decompress( product_page[ 'product_page_meta' ][ 0 ][ 'compressed_content' ] ),
                    features='lxml'
                )
                
                product_title = content.head.title.text
                
                try:
                    product_image = content.find( "div", {
                        "id": "main-image-container"
                    } ).find( 'li', { 'class', 'image' } ).find( 'img' )[ 'src' ]
                except:
                    product_image = ''
                
                # print( product_image )
                
                await self.__amazon_products.update_one(
                    {
                        '_id': product_page[ '_id' ]
                    },
                    {
                        '$set': {
                            'updated_at.last_parsed_at': now_time()
                        }
                    }
                )
                
                await self.__amazon_products_meta.update_one(
                    {
                        'amazon_product_id': str( product_page[ '_id' ] )
                    },
                    {
                        '$set': {
                            'metas.product_name' : product_title,
                            'metas.product_image': product_image,
                        }
                    }
                )
            
            await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) )
    
    async def __check_links_in_guest_posts( self ):
        print( 'Links in guest post checking started...' )
        
        while True:
            pipeline = [
                {
                    "$lookup": {
                        "from"    : "pages",
                        
                        "let"     : {
                            # in let get parent vars
                            "guest_post_url": "$guest_post_url"
                        },
                        
                        "pipeline": [
                            {
                                "$match": {
                                    # use $$ to use the let var & $ to use lookups collection
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [ "$$guest_post_url", '$url' ]
                                            },
                                            {
                                                '$gt': [ '$updated_at.last_parsed_at', '1' ]
                                            }
                                        ]
                                    }
                                },
                            }
                        ],
                        "as"      : "page"
                    }
                },
                {
                    '$unwind': '$page'
                },
                
                {
                    "$lookup": {
                        "from"    : "pages_outbound_links",
                        
                        "let"     : {
                            # in let get parent vars
                            "main_page_id": {
                                '$toString': "$page._id"
                            }
                        },
                        
                        "pipeline": [
                            {
                                "$match": {
                                    # use $$ to use the let var & $ to use lookups collection
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [ "$$main_page_id", '$page_id' ]
                                            }
                                        ]
                                    }
                                },
                            }
                        ],
                        "as"      : "pages_outbound_links"
                    }
                },
                
                {
                    "$sort": {
                        "updated_at.link_last_checked_at": 1
                    }
                },
                {
                    "$limit": 10
                },
            ]
            
            async for link_in_guest_post in self.__links_in_guest_posts.aggregate( pipeline ):
                guest_url_found = False
                
                for outbound_link in link_in_guest_post[ 'pages_outbound_links' ]:
                    if link_in_guest_post[ 'holding_url' ] == outbound_link[ 'url' ]:
                        guest_url_found = True
                        break
                
                if guest_url_found:
                    await self.__links_in_guest_posts.update_one(
                        {
                            '_id': link_in_guest_post[ '_id' ]
                        },
                        {
                            '$set': {
                                'link_infos.exists'              : '1',
                                'updated_at.link_last_checked_at': now_time()
                            }
                        }
                    )
                else:
                    await self.__links_in_guest_posts.update_one(
                        {
                            '_id': link_in_guest_post[ '_id' ]
                        },
                        {
                            '$set': {
                                'link_infos.exists'              : '0',
                                'updated_at.link_last_checked_at': now_time()
                            }
                        }
                    )
            
            await asyncio.sleep( int( os.getenv( 'SLEEP_TIME' ) ) + 5 )
    
    async def __do_side_by_side_works( self ):
        print( 'Starting side by side working instances' )
        
        # await asyncio.gather(
        #
        # )
    
    # async def __check_domain_uptime( self ):
    #     print('Domain Uptime check started...')
    #     user_agent = {
    #         'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/55.0.2883.87 '
    #     }
    #
    #     while True:
    #         pipeline = [
    #
    #         ]
    #
    #         async for user_domain in self.__users_domains.aggregate(pipeline):
    #             print(user_domain)


print( 'Initiating Process' )

Scrapper()

print( 'All Done' )  # but it won't be called ever
