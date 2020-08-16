import os
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import aiohttp
import pprint

from urllib import parse
from bs4 import BeautifulSoup
import zlib

import time
from datetime import datetime

from dotenv import load_dotenv

import helpers


load_dotenv()


class Parser:
    def __init__(self):
        self.__init_variables()  # load & initiate variables
        self.__init_actions()  # run initiate action

        asyncio.get_event_loop().run_until_complete(
            self.__run_main()
        )  # init main actions

    def __init_variables(self):
        pass

    def __init_actions(self):
        self.__connect_db()

    def __connect_db(self):
        self.__db_connection = AsyncIOMotorClient(os.getenv("MONGODB_CONNECTION"))

        self.__db = self.__db_connection.amz_watch

        self.__users_domains = self.__db.users_domains
        self.__pages = self.__db.pages
        self.__pages_inbound_links = self.__db.pages_inbound_links
        self.__pages_outbound_links = self.__db.pages_outbound_links
        self.__amazon_products = self.__db.amazon_products
        self.__amazon_products_meta = self.__db.amazon_products_meta
        self.__amazon_products_in_pages = self.__db.amazon_products_in_pages

    async def __run_main(self):
        print("Starting...")

        await asyncio.gather(self.__run_parser())

    async def __run_parser(self):
        print("Starting parser working instances")

        await asyncio.gather(
            self.__parse_page(), self.__parse_product_page(),
        )

    async def __parse_page(self):
        # handle page parsing
        print("Page parsing started...")

        while True:
            print("Getting pages for parsing... " + time.ctime())

            pipeline = [
                {
                    "$match": {
                        "$expr": {
                            "$lt": [
                                {
                                    "$sum": [
                                        {
                                            "$convert": {
                                                "input": "$updated_at.last_parsed_at",
                                                "to": "double",
                                                "onError": 0,
                                                "onNull": 0,
                                            }
                                        },
                                        21600 * 1000,
                                    ]
                                },
                                helpers.now_time_integer(),
                            ]
                        }
                    }
                },
                {
                    "$lookup": {
                        "from": "pages_meta",
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
                                            {"$eq": ["$$id", "$page_id"]},
                                            {
                                                "$ne": [
                                                    {"$type": "$compressed_content"},
                                                    "missing",
                                                ]
                                            },
                                            {
                                                "$ne": [
                                                    {"$type": "$page_status"},
                                                    "missing",
                                                ]
                                            },
                                            {
                                                "$in": [
                                                    {
                                                        "$substr": [
                                                            {
                                                                "$toString": "$page_status"
                                                            },
                                                            0,
                                                            1,
                                                        ]
                                                    },
                                                    ["2", "3", "8"],
                                                ]
                                            },
                                        ]
                                    }
                                },
                            }
                        ],
                        "as": "page_meta",
                    }
                },
                {"$match": {"$expr": {"$gt": [{"$size": "$page_meta"}, 0]},}},
                {
                    "$lookup": {
                        "from": "domains",
                        "let": {
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
                                                "$eq": [
                                                    "$$domain_id",
                                                    {"$toString": "$_id"},
                                                ]
                                            }
                                        ]
                                    }
                                },
                            }
                        ],
                        "as": "domain",
                    }
                },
                {"$unwind": "$domain"},
                {"$sort": {"updated_at.last_parsed_at": 1}},
                {"$limit": 10},
            ]

            async for page in self.__pages.aggregate(pipeline):
                print("Parsing page " + page["url"])

                content = BeautifulSoup(
                    zlib.decompress(page["page_meta"][0]["compressed_content"]),
                    features="lxml",
                )

                domain_use_for = await self.__get_domain_use_for_from_domain_id(
                    page["domain_id"]
                )

                inbound_links = []
                outbound_links = []

                for raw_link in content.find_all("a", href=True):
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

                    joined_url = parse.urljoin(page["domain"]["url"], raw_link["href"])

                    parsed_page_base = parse.urlparse(page["url"])
                    parsed_joined_url = parse.urlparse(joined_url)

                    if (
                        parsed_joined_url[0] in ["http", "https"]
                        and parsed_joined_url[1]
                        and (
                            not parsed_joined_url[2]
                            or (
                                (
                                    parsed_joined_url[2]
                                    and not len(parsed_joined_url.path.split("."))
                                )
                                or (
                                    parsed_joined_url[2]
                                    and len(parsed_joined_url.path.split("."))
                                    and parsed_joined_url.path.split(".")[-1]
                                    not in [
                                        "jpg",
                                        "jpeg",
                                        "png",
                                        "zip",
                                        "pdf",
                                        "doc",
                                        "gif",
                                    ]
                                )
                            )
                        )
                    ):
                        final_url = parse.urlunparse(
                            (
                                parsed_joined_url.scheme,
                                parsed_joined_url.netloc,
                                parsed_joined_url.path,
                                "",
                                "",
                                "",
                            )
                        )

                        if (
                            parsed_page_base[0] == parsed_joined_url[0]
                            and parsed_page_base[1] == parsed_joined_url[1]
                        ):
                            if {"link": final_url} not in inbound_links:
                                inbound_links.append({"link": final_url})
                        else:
                            if {"link": final_url} not in outbound_links:
                                outbound_links.append({"link": final_url})

                        # print( parse.urlparse( joined_url ) )

                if domain_use_for and (
                    "broken_links_check_service" in domain_use_for
                    or "amazon_products_check_service" in domain_use_for
                    or "pages_speed_check_service" in domain_use_for
                ):
                    for inbound_link in inbound_links:
                        await self.__pages.update_one(
                            {
                                "domain_id": str(page["domain"]["_id"]),
                                "url": inbound_link["link"],
                            },
                            {
                                "$setOnInsert": {
                                    "domain_id": str(page["domain"]["_id"]),
                                    "url": inbound_link["link"],
                                    "updated_at": {"last_scraped_at": "2"},
                                }
                            },
                            upsert=True,
                        )

                if domain_use_for and "amazon_products_check_service" in domain_use_for:
                    prev_original_product_urls = await self.__get_previous_products_in_pages(
                        str(page["_id"])
                    )
                    current_original_product_urls = [
                        outbound_link["link"] for outbound_link in outbound_links
                    ]

                    for_deletes = list(
                        set(prev_original_product_urls)
                        - set(current_original_product_urls)
                    )

                    for for_delete in for_deletes:
                        await self.__amazon_products_in_pages.delete_one(
                            {
                                "page_id": str(page["_id"]),
                                "original_product_url": for_delete,
                            }
                        )

                # add inbound links & outbound links
                await self.__insert_inbound_links(
                    str(page["_id"]),
                    [inbound_link["link"] for inbound_link in inbound_links],
                )
                await self.__insert_outbound_links(
                    str(page["_id"]),
                    [outbound_link["link"] for outbound_link in outbound_links],
                )

                await self.__pages.update_one(
                    {"_id": page["_id"]},
                    {"$set": {"updated_at.last_parsed_at": helpers.now_time()}},
                )

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")))

    async def __get_domain_use_for_from_domain_id(self, domain_id):
        pipeline = [
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$domain_id", domain_id]}
                            # check status also if project is still running
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$domain_id",
                    "domain_use_for": {"$mergeObjects": "$domain_use_for"},
                }
            },
        ]

        async for user_domain in self.__users_domains.aggregate(pipeline):
            return user_domain["domain_use_for"]

    async def __insert_inbound_links(self, page_id, links):
        prev_links = await self.__get_previous_inbound_links(page_id)

        if not prev_links:
            if links:
                await self.__pages_inbound_links.insert_many(
                    [{"page_id": page_id, "url": link} for link in links]
                )
        else:
            for_deletes = list(set(prev_links) - set(links))
            for_inserts = list(set(links) - set(prev_links))

            for for_delete in for_deletes:
                await self.__pages_inbound_links.delete_one(
                    {"page_id": page_id, "url": for_delete}
                )

            if for_inserts:
                await self.__pages_inbound_links.insert_many(
                    [
                        {"page_id": page_id, "url": for_insert}
                        for for_insert in for_inserts
                    ]
                )

    async def __insert_outbound_links(self, page_id, links):
        prev_links = await self.__get_previous_outbound_links(page_id)

        if not prev_links:
            if links:
                await self.__pages_outbound_links.insert_many(
                    [{"page_id": page_id, "url": link} for link in links]
                )
        else:
            for_deletes = list(set(prev_links) - set(links))
            for_inserts = list(set(links) - set(prev_links))

            for for_delete in for_deletes:
                await self.__pages_outbound_links.delete_one(
                    {"page_id": page_id, "url": for_delete}
                )

            if for_inserts:
                await self.__pages_outbound_links.insert_many(
                    [
                        {"page_id": page_id, "url": for_insert}
                        for for_insert in for_inserts
                    ]
                )

    async def __get_previous_inbound_links(self, page_id):
        pipeline = [{"$match": {"$expr": {"$and": [{"$eq": ["$page_id", page_id]}]}},}]

        prev_inbound_links = []

        async for inbound_link in self.__pages_inbound_links.aggregate(pipeline):
            prev_inbound_links.append(inbound_link["url"])

        return prev_inbound_links

    async def __get_previous_outbound_links(self, page_id):
        pipeline = [{"$match": {"$expr": {"$and": [{"$eq": ["$page_id", page_id]}]}},}]

        prev_outbound_links = []

        async for inbound_link in self.__pages_outbound_links.aggregate(pipeline):
            prev_outbound_links.append(inbound_link["url"])

        return prev_outbound_links

    async def __get_previous_products_in_pages(self, page_id):
        pipeline = [{"$match": {"$expr": {"$and": [{"$eq": ["$page_id", page_id]}]}}}]

        actual_product_url = []

        async for product_in_page in self.__amazon_products_in_pages.aggregate(
            pipeline
        ):
            actual_product_url.append(product_in_page["actual_product_url"])

        return actual_product_url

    async def __parse_product_page(self):
        # handle product page parsing
        print("Product page parsing started...")

        while True:
            print("Getting products for parsing... " + time.ctime())

            pipeline = [
                {
                    "$match": {
                        "$expr": {
                            "$lt": [
                                {
                                    "$sum": [
                                        {
                                            "$convert": {
                                                "input": "$updated_at.last_parsed_at",
                                                "to": "double",
                                                "onError": 0,
                                                "onNull": 0,
                                            }
                                        },
                                        21600 * 1000,
                                    ]
                                },
                                helpers.now_time_integer(),
                            ]
                        }
                    }
                },
                {
                    "$lookup": {
                        "from": "amazon_products_meta",
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
                                            {"$eq": ["$$id", "$amazon_product_id"]},
                                            {
                                                "$ne": [
                                                    {"$type": "$compressed_content"},
                                                    "missing",
                                                ]
                                            },
                                            {
                                                "$ne": [
                                                    {"$type": "$page_status"},
                                                    "missing",
                                                ]
                                            },
                                            {
                                                "$in": [
                                                    {
                                                        "$substr": [
                                                            {
                                                                "$toString": "$page_status"
                                                            },
                                                            0,
                                                            1,
                                                        ]
                                                    },
                                                    ["2", "3", "8"],
                                                ]
                                            },
                                        ]
                                    }
                                },
                            }
                        ],
                        "as": "product_page_meta",
                    }
                },
                {"$unwind": "$product_page_meta"},
                {"$sort": {"updated_at.last_parsed_at": 1}},
                {"$limit": 100},
            ]

            async for product_page in self.__amazon_products.aggregate(pipeline):
                print("Parsing product page " + product_page["url"])

                content = BeautifulSoup(
                    zlib.decompress(
                        product_page["product_page_meta"]["compressed_content"]
                    ),
                    features="lxml",
                )

                parse_error = ""
                product_title = ""
                product_image = ""
                in_stock = "0"

                try:
                    product_title = content.head.title.text

                    product_image = (
                        content.find("div", {"id": "main-image-container"})
                        .find("li", {"class", "image"})
                        .find("img")["src"]
                    )

                    add_to_cart_div = content.find("form", id="addToCart")

                    if add_to_cart_div:
                        availability = add_to_cart_div.find("div", id="availability")

                        outOfStock = add_to_cart_div.find("div", id="outOfStock")

                        if availability:
                            in_stock = "1"
                        elif outOfStock:
                            in_stock = "0"
                        else:
                            parse_error = "cart_area"
                    else:
                        parse_error = "cart_area"

                        print("Cart area not found for " + product_page["url"])
                except:
                    parse_error = "body_area"

                    print(
                        "Something went wrong when parsing product page "
                        + product_page["url"]
                    )

                if parse_error:
                    await self.__amazon_products_meta.update_one(
                        {"amazon_product_id": str(product_page["_id"])},
                        {"$set": {"page_status": 888,}},
                    )

                # print( product_image )

                await self.__amazon_products.update_one(
                    {"_id": product_page["_id"]},
                    {"$set": {"updated_at.last_parsed_at": helpers.now_time()}},
                )

                await self.__amazon_products_meta.update_one(
                    {"amazon_product_id": str(product_page["_id"])},
                    {
                        "$set": {
                            "metas.product_name": product_title,
                            "metas.product_image": product_image,
                            "metas.in_stock": in_stock,
                        }
                    },
                )

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")))


print("Initiating Parser Process")

Parser()

print("All Done Parsing")  # but it won't be called ever
