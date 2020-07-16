import os
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.collection import ReturnDocument
import aiohttp
import pprint

from urllib import parse
import tldextract

import time
from datetime import datetime

from dotenv import load_dotenv

import helpers


load_dotenv()


class SideWorker:
    def __init__(self):
        self.__init_variables()  # load & initiate variables
        self.__init_actions()  # run initiate action

        asyncio.get_event_loop().run_until_complete(
            self.__run_main()
        )  # init main actions

    def __init_variables(self):
        self.__req_header = {
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/45.0.2454.101 Safari/537.36"
            ),
        }

    def __init_actions(self):
        self.__connect_db()

    def __connect_db(self):
        self.__db_connection = AsyncIOMotorClient(os.getenv("MONGODB_CONNECTION"))

        self.__db = self.__db_connection.amz_watch

        self.__domains = self.__db.domains
        self.__pages = self.__db.pages
        self.__pages_outbound_links = self.__db.pages_outbound_links
        self.__amazon_products = self.__db.amazon_products
        self.__amazon_products_in_pages = self.__db.amazon_products_in_pages
        self.__links_in_guest_posts = self.__db.links_in_guest_posts

    async def __run_main(self):
        print("Starting side worker instances...")

        await asyncio.gather(self.__run_side_worker())

    async def __run_side_worker(self):
        await asyncio.gather(
            self.__assign_domain_url_to_pages(),
            self.__assign_guest_post_url_to_pages(),
            self.__check_links_in_guest_posts(),
            self.__check_and_assign_to_tables_if_outbound_link_is_product_page(),
        )

    async def __assign_domain_url_to_pages(self):
        # it will take domain url & assign to pages collection for scrape if not has in there
        # no need to assign guest domain url
        while True:
            print("Checking domain url for insert into pages table... " + time.ctime())

            pipeline = [
                {
                    "$lookup": {
                        "from": "users_domains",
                        "let": {
                            # in let get parent vars
                            "id": {"$toString": "$_id"}
                        },
                        "pipeline": [
                            {
                                "$match": {
                                    # use $$ to use the let var & $ to use lookups collection
                                    "$expr": {
                                        "$and": [
                                            {"$eq": ["$$id", "$domain_id"]},
                                            {
                                                "$or": [
                                                    # if need with status wrap each with $and & check status also
                                                    {
                                                        "$ne": [
                                                            {
                                                                "$type": "$domain_use_for.broken_links_check_service"
                                                            },
                                                            "missing",
                                                        ]
                                                    },
                                                    {
                                                        "$ne": [
                                                            {
                                                                "$type": "$domain_use_for.amazon_products_check_service"
                                                            },
                                                            "missing",
                                                        ]
                                                    },
                                                    {
                                                        "$ne": [
                                                            {
                                                                "$type": "$domain_use_for.pages_speed_check_service"
                                                            },
                                                            "missing",
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    }
                                },
                            }
                        ],
                        "as": "user_domains",
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
                                    "$or": [
                                        {"$eq": [{"$type": "$updated_at"}, "missing",]},
                                        {
                                            "$eq": [
                                                {
                                                    "$type": "$updated_at.domain_url_in_page_last_check_at"
                                                },
                                                "missing",
                                            ]
                                        },
                                        {
                                            "$eq": [
                                                "$updated_at.domain_url_in_page_last_check_at",
                                                "",
                                            ]
                                        },
                                    ]
                                },
                                {"$gt": [{"$size": "$user_domains"}, 0]},
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
                {"$sort": {"updated_at.domain_url_in_page_last_check_at": 1}},
                {"$limit": 1},
            ]

            async for domain in self.__domains.aggregate(pipeline):
                # print( domain )

                # if need use transaction
                await self.__pages.update_one(
                    {"domain_id": str(domain["_id"]), "url": domain["url"]},
                    {
                        "$setOnInsert": {
                            "domain_id": str(domain["_id"]),
                            "url": domain["url"],
                            "updated_at": {"last_scraped_at": "1"},
                        }
                    },
                    upsert=True,
                )
                await self.__domains.update_one(
                    {"_id": domain["_id"],},
                    {
                        "$set": {
                            "updated_at": {
                                "domain_url_in_page_last_check_at": helpers.now_time()
                            }
                        }
                    },
                    upsert=True,
                )

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")) + 10)

    async def __assign_guest_post_url_to_pages(self):
        while True:
            print(
                "Checking guest post url for insert into pages table... " + time.ctime()
            )

            pipeline = [
                {
                    "$match": {
                        "$expr": {
                            "$or": [
                                {"$eq": [{"$type": "$updated_at"}, "missing"]},
                                {
                                    "$eq": [
                                        {
                                            "$type": "$updated_at.guest_post_url_in_page_last_checked_at"
                                        },
                                        "missing",
                                    ]
                                },
                                {
                                    "$eq": [
                                        "$updated_at.guest_post_url_in_page_last_checked_at",
                                        "",
                                    ]
                                },
                                {
                                    "$eq": [
                                        "$updated_at.guest_post_url_in_page_last_checked_at",
                                        "1",
                                    ]
                                },
                            ]
                        }
                    }
                },
                {
                    "$lookup": {
                        "from": "users_domains",
                        "let": {
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
                                                    {"$toString": "$_id"},
                                                ]
                                            },
                                        ]
                                    }
                                },
                            }
                        ],
                        "as": "user_domain",
                    }
                },
                {"$sort": {"updated_at.guest_post_url_in_page_last_checked_at": 1}},
                {"$limit": 10},
            ]

            async for guest_post in self.__links_in_guest_posts.aggregate(pipeline):
                # print(guest_post)
                service_info = guest_post["user_domain"][0]["domain_use_for"][
                    "guest_posts_check_service"
                ]
                guest_domain_id = service_info["guest_domain_id"]
                guest_post_url = guest_post["guest_post_url"]

                # if need use transaction
                await self.__pages.update_one(
                    {"domain_id": guest_domain_id, "url": guest_post_url},
                    {
                        "$setOnInsert": {
                            "domain_id": guest_domain_id,
                            "url": guest_post_url,
                            "updated_at": {"last_scraped_at": "1"},
                        }
                    },
                    upsert=True,
                )

                await self.__links_in_guest_posts.update_one(
                    {"_id": guest_post["_id"]},
                    {
                        "$set": {
                            "updated_at.guest_post_url_in_page_last_checked_at": helpers.now_time()
                        }
                    },
                )

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")) + 10)

    async def __check_links_in_guest_posts(self):
        while True:
            print("Getting links for check Links in guest post... " + time.ctime())

            pipeline = [
                {
                    "$lookup": {
                        "from": "pages",
                        "let": {
                            # in let get parent vars
                            "guest_post_url": "$guest_post_url"
                        },
                        "pipeline": [
                            {
                                "$match": {
                                    # use $$ to use the let var & $ to use lookups collection
                                    "$expr": {
                                        "$and": [
                                            {"$eq": ["$$guest_post_url", "$url"]},
                                            {
                                                "$gt": [
                                                    {
                                                        "$convert": {
                                                            "input": "$updated_at.last_parsed_at",
                                                            "to": "double",
                                                            "onError": 0,
                                                            "onNull": 0,
                                                        }
                                                    },
                                                    1,
                                                ]
                                            },
                                        ]
                                    }
                                },
                            }
                        ],
                        "as": "page",
                    }
                },
                {"$unwind": "$page"},
                {
                    "$lookup": {
                        "from": "pages_outbound_links",
                        "let": {
                            # in let get parent vars
                            "main_page_id": {"$toString": "$page._id"}
                        },
                        "pipeline": [
                            {
                                "$match": {
                                    # use $$ to use the let var & $ to use lookups collection
                                    "$expr": {
                                        "$and": [
                                            {"$eq": ["$$main_page_id", "$page_id"]}
                                        ]
                                    }
                                },
                            }
                        ],
                        "as": "pages_outbound_links",
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
                                                        "input": "$updated_at.link_last_checked_at",
                                                        "to": "double",
                                                        "onError": 0,
                                                        "onNull": 0,
                                                    }
                                                },
                                                10800 * 1000,
                                            ]
                                        },
                                        helpers.now_time_integer(),
                                    ]
                                },
                            ]
                        }
                    }
                },
                {"$sort": {"updated_at.link_last_checked_at": 1}},
                {"$limit": 10},
            ]

            async for link_in_guest_post in self.__links_in_guest_posts.aggregate(
                pipeline
            ):
                print("Checking link exist for " + link_in_guest_post["holding_url"])

                guest_url_found = False

                for outbound_link in link_in_guest_post["pages_outbound_links"]:
                    if link_in_guest_post["holding_url"] == outbound_link["url"]:
                        guest_url_found = True
                        break

                if guest_url_found:
                    await self.__links_in_guest_posts.update_one(
                        {"_id": link_in_guest_post["_id"]},
                        {
                            "$set": {
                                "link_infos.exists": "1",
                                "updated_at.link_last_checked_at": helpers.now_time(),
                            }
                        },
                    )
                else:
                    await self.__links_in_guest_posts.update_one(
                        {"_id": link_in_guest_post["_id"]},
                        {
                            "$set": {
                                "link_infos.exists": "0",
                                "updated_at.link_last_checked_at": helpers.now_time(),
                            }
                        },
                    )

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")) + 5)

    async def __check_and_assign_to_tables_if_outbound_link_is_product_page(self):
        while True:
            print("Getting urls for product page check... " + time.ctime())

            pipeline = [
                {
                    "$lookup": {
                        "from": "pages",
                        "let": {"page_id": "$page_id"},
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [
                                                    "$$page_id",
                                                    {"$toString": "$_id"},
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }
                        ],
                        "as": "page",
                    }
                },
                {"$unwind": "$page"},
                {
                    "$lookup": {
                        "from": "users_domains",
                        "let": {"domain_id": "$page.domain_id"},
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {
                                        "$and": [
                                            {"$eq": ["$$domain_id", "$domain_id"]},
                                            {
                                                "$ne": [
                                                    {"$type": "$domain_use_for"},
                                                    "missing",
                                                ]
                                            },
                                            {
                                                "$ne": [
                                                    {
                                                        "$type": "$domain_use_for.amazon_products_check_service"
                                                    },
                                                    "missing",
                                                ]
                                            },
                                            {
                                                "$ne": [
                                                    {
                                                        "$type": "$domain_use_for.amazon_products_check_service.status"
                                                    },
                                                    "missing",
                                                ]
                                            },
                                            {
                                                "$eq": [
                                                    "$domain_use_for.amazon_products_check_service.status",
                                                    "active",
                                                ]
                                            },
                                        ]
                                    }
                                }
                            }
                        ],
                        "as": "user_domains",
                    }
                },
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$gt": [{"$size": "$user_domains"}, 0]},
                                {
                                    "$lt": [
                                        {
                                            "$sum": [
                                                {
                                                    "$convert": {
                                                        "input": "$other_info.updated_at.head_request_for_is_product_page_last_checked_at",
                                                        "to": "double",
                                                        "onError": 0,
                                                        "onNull": 0,
                                                    }
                                                },
                                                86400 * 1000,
                                            ]
                                        },
                                        helpers.now_time_integer(),
                                    ]
                                },
                            ]
                        }
                    }
                },
                {
                    "$sort": {
                        "other_info.updated_at.head_request_for_is_product_page_last_checked_at": 1
                    }
                },
                {"$limit": 3},
            ]

            head_req_list = []

            async for link in self.__pages_outbound_links.aggregate(pipeline):
                head_req_list.append(
                    self.__head_request_for_is_product_page_check(link["url"], link)
                )

            if len(head_req_list):
                await asyncio.gather(*head_req_list)

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")) + 5)

    async def __head_request_for_is_product_page_check(self, url, url_obj):
        # currently can't req for head for these https://amzn.to/2Jv8ICC
        # cz 405 method not allowd giving

        print("Checking " + url + " for product page")

        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False)
        ) as aiohttp_session:
            async with aiohttp_session.get(
                url, allow_redirects=True, headers=self.__req_header
            ) as res:
                final_url = str(res.url)

                if str(res.status)[0] in ["2", "3"]:
                    if tldextract.extract(final_url).domain == "amazon":
                        parsed_url = parse.urlparse(final_url)

                        only_product_url = parse.urlunparse(
                            (
                                parsed_url.scheme,
                                parsed_url.netloc,
                                parsed_url.path,
                                "",
                                "",
                                "",
                            )
                        )

                        product = await self.__amazon_products.find_one_and_update(
                            {"url": only_product_url},
                            {"$setOnInsert": {"url": only_product_url}},
                            upsert=True,
                            return_document=ReturnDocument.AFTER,
                        )

                        for user_domain in url_obj["user_domains"]:
                            await self.__amazon_products_in_pages.update_one(
                                {
                                    "product_id": str(product["_id"]),
                                    "page_id": url_obj["page_id"],
                                    "user_domain_id": str(user_domain["_id"]),
                                    "original_url": url,
                                },
                                {
                                    "$setOnInsert": {
                                        "product_id": str(product["_id"]),
                                        "page_id": url_obj["page_id"],
                                        "user_domain_id": str(user_domain["_id"]),
                                        "original_url": url,
                                    },
                                    "$set": {"actual_product_url": final_url,},
                                },
                                upsert=True,
                            )

                # update same urls otherwise it will take n check same url
                await self.__pages_outbound_links.update_one(
                    {"_id": url_obj["_id"]},
                    {
                        "$set": {
                            "other_info.updated_at.head_request_for_is_product_page_last_checked_at": helpers.now_time()
                        }
                    },
                )


print("Initiating Side Worker Process")

SideWorker()

print("All Done Side Working")  # but it won't be called ever
