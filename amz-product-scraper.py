import os
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.collection import ReturnDocument
import aiohttp
import pprint

from pyppeteer import launch
import zlib

from bs4 import BeautifulSoup

import time
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()


def now_time_integer():
    return int(time.time() * 1000)


def now_time():
    return str(now_time_integer())


class ProductScraper:
    def __init__(self):
        self.__init_variables()  # load & initiate variables
        self.__init_actions()  # run initiate action

        asyncio.get_event_loop().run_until_complete(
            self.__run_main()
        )  # init main actions

    def __init_variables(self):
        self.__browser_instance_limit = 1
        self.__page_instance_limit = 1
        self.__urls_are_set_into_page_instances = False

        self.__browser_instances = {}

        self.__pages_hanged_browsers = []  # keep browser list if a page hang

    def __init_actions(self):
        self.__connect_db()

        # get current running instance id
        asyncio.get_event_loop().run_until_complete(self.__get_process_instance_id())

    def __connect_db(self):
        self.__db_connection = AsyncIOMotorClient(os.getenv("MONGODB_CONNECTION"))

        self.__db = self.__db_connection.amz_watch

        self.__process_instances_collection = self.__db.system_process_instances
        self.__browser_instances_collection = self.__db.system_browser_instances
        self.__page_instances_collection = self.__db.system_page_instances
        self.__users_domains = self.__db.users_domains
        self.__pages = self.__db.pages
        self.__pages_meta = self.__db.pages_meta
        self.__amazon_products = self.__db.amazon_products
        self.__amazon_products_meta = self.__db.amazon_products_meta

    async def __get_process_instance_id(self):
        # save instance info when script run
        temp_process_instance_id = await self.__process_instances_collection.insert_one(
            {
                "browser_instance_limit": self.__browser_instance_limit,
                "page_instance_limit": self.__page_instance_limit,
                "process_started_at": datetime.utcnow(),
                "process_updated_at": "",
                "process_closed_at": "",
                "worker_info": {"work": "product_scrape"},
            }
        )

        self.__process_instance_id = str(temp_process_instance_id.inserted_id)

        print("Process instance id ----> " + self.__process_instance_id)

    async def __run_main(self):
        print("Starting...")

        await asyncio.gather(
            # self.__get_instances_info(),
            # self.__update_instances_info(),
            self.__handle_instances(),
            self.__run_scraper(),
        )

    async def __get_instances_info(self):
        # get instance infos
        pass

    async def __update_instances_info(self):
        # run forever for get instance change
        pass

    async def __handle_instances(self):
        # handle all instance from here
        await asyncio.gather(
            self.__handle_process_instances(),
            self.__handle_browser_instances(),
            self.__handle_page_instances(),
        )

    async def __handle_process_instances(self):
        # handle process instance
        # if for close this instance then close it
        await asyncio.gather(self.__close_process_instance())

    async def __close_process_instance(self):
        # handle process close & its related others
        pass

    async def __handle_browser_instances(self):
        # call for browser start & close actions
        await asyncio.gather(
            self.__start_browser_instance(), self.__close_browser_instance()
        )

    async def __start_browser_instance(self):
        while True:
            # if our browser instance is less then the limit then start
            if len(self.__browser_instances) < self.__browser_instance_limit:
                try:
                    temp_browser = await launch(headless=True, args=["--no-sandbox"],)
                    # temp_browser = 'temp_browser'
                    browser_id = await self.__get_browser_instance_id()

                    self.__browser_instances[browser_id] = {
                        "browser": temp_browser,
                        "pages": {},
                    }

                    print("Created browser instance -----> " + browser_id)
                except:
                    print("Some problem when creating browser instance")

                    await asyncio.sleep(30)

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")))

    async def __get_browser_instance_id(self):
        browser_instance_id = await self.__browser_instances_collection.insert_one(
            {
                "process_id": self.__process_instance_id,
                "browser_started_at": datetime.utcnow(),
                "browser_closed_at": "",
            }
        )

        return str(browser_instance_id.inserted_id)

    async def __close_browser_instance(self):
        while True:
            # if exceed the limit close browser instance until its equal to the limit
            if len(self.__browser_instances) > self.__browser_instance_limit:
                await self.__close_a_browser_instance()

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")))

    async def __close_a_browser_instance(self, instance_id=None):
        # handle closing a browser instance forcefully if id present or close randomly
        pass

    async def __update_browser_close_time(self):
        # update browser instance close info
        pass

    async def __handle_page_instances(self):
        # call for page start & close actions
        await asyncio.gather(self.__start_page_instance(), self.__close_page_instance())

    async def __start_page_instance(self):
        while True:
            # handle each browsers page start by limit
            for browser in self.__browser_instances:
                # currently we r using same page limit for all browser
                if (
                    len(self.__browser_instances[browser]["pages"])
                    < self.__page_instance_limit
                ):
                    temp_page = await self.__browser_instances[browser][
                        "browser"
                    ].newPage()

                    await temp_page.setUserAgent(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"
                    )

                    # temp_page = 'temp page'
                    page_id = await self.__get_page_instance_id(browser)

                    self.__browser_instances[browser]["pages"][page_id] = {
                        "page": temp_page,
                        "working_status": "false",
                        "info": {},
                    }

                    print("Created page instance -----> " + page_id)

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")))

    async def __get_page_instance_id(self, browser_id):
        page_instance_id = await self.__page_instances_collection.insert_one(
            {
                "browser_id": browser_id,
                "page_started_at": datetime.utcnow(),
                "page_closed_at": "",
            }
        )

        return str(page_instance_id.inserted_id)

    async def __close_page_instance(self):
        while True:
            # handle each browsers page close by limit
            for browser in self.__browser_instances:
                # currently we r using same page limit for all browser
                if (
                    len(self.__browser_instances[browser]["pages"])
                    > self.__page_instance_limit
                ):
                    await self.__close_a_page_instance(browser)

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")))

    async def __close_a_page_instance(self, browser_id, page_id=None):
        # handle randomly close a page by browser or forcefully by page_id for that browser
        # browser_id must needed
        pass

    async def __update_page_close_time(self):
        # update page instance close info
        pass

    async def __run_scraper(self):
        print("Starting product scraper working instances")

        await asyncio.gather(
            self.__do_before_scraping(), self.__do_scraping(),
        )

    async def __do_before_scraping(self):
        await asyncio.gather(self.__get_and_assign_product_pages_to_instances())

    async def __get_and_assign_product_pages_to_instances(self):
        # get pages from collection by limit then assign these into browser pages
        # if all pages status are false then only run this
        await asyncio.sleep(10)
        while True:
            # pprint.pprint(self.__browser_instances)
            if (
                not self.__urls_are_set_into_page_instances
                and not self.__pages_hanged_browsers
            ):
                # we r safe cz if limit changes midway no harm for this block
                print("getting products for scraping " + time.ctime())

                get_pages_upto = (
                    self.__browser_instance_limit * self.__page_instance_limit
                )

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
                                                            "input": "$updated_at.last_scraped_at",
                                                            "to": "double",
                                                            "onError": 0,
                                                            "onNull": 0,
                                                        }
                                                    },
                                                    86400 * 1000,
                                                ]
                                            },
                                            int(time.time() * 1000),
                                        ]
                                    },
                                ]
                                # handle status & other match
                            }
                        }
                    },
                    {"$sort": {"updated_at.last_scraped_at": 1}},
                    {
                        # limit only 1 so that we only scrape 1 products at a time
                        "$limit": 1
                    },
                ]

                async for product_page in self.__amazon_products.aggregate(
                    pipeline_for_product_pages
                ):
                    self.__assign_pages_into_instances(
                        product_page["url"], product_page
                    )

                self.__urls_are_set_into_page_instances = True

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")))

    def __assign_pages_into_instances(self, url, other_info):
        # assign tem cz __browser_instances can be changed anytime
        temp_browser_instance = self.__browser_instances

        for browser in temp_browser_instance:
            for page in temp_browser_instance[browser]["pages"]:
                # check if exist main cz these can change midway
                if (
                    temp_browser_instance[browser]["pages"][page]["working_status"]
                    == "false"
                    and self.__browser_instances[browser]
                    and self.__browser_instances[browser]["pages"][page]
                    and not self.__check_url_if_already_has_in_instances(url)
                ):
                    self.__browser_instances[browser]["pages"][page]["info"] = {
                        "url": url,
                        "other_info": other_info,
                    }
                    self.__browser_instances[browser]["pages"][page][
                        "working_status"
                    ] = "pending"

                    # assign to page_instance_urls if track url

                    return  # it will break from parents also

    def __check_url_if_already_has_in_instances(self, url):
        # check if url has in instance_container
        for browser in self.__browser_instances:
            for page in self.__browser_instances[browser]["pages"]:
                if (
                    self.__browser_instances[browser]
                    and self.__browser_instances[browser]["pages"][page]
                    and self.__browser_instances[browser]["pages"][page]["info"]
                    and self.__browser_instances[browser]["pages"][page]["info"]["url"]
                    == url
                ):
                    return True

        return False

    async def __do_scraping(self):
        await asyncio.gather(self.__start_scrapping())

    async def __start_scrapping(self):
        # get all pages and insert into asyncio.gather
        # after all page finish update status so that new pages can be assigned into pages instance
        while True:
            if self.__urls_are_set_into_page_instances:
                temp_browser_instance = self.__browser_instances
                task_list = []

                for browser in temp_browser_instance:
                    for page in temp_browser_instance[browser]["pages"]:
                        page_instance_data = temp_browser_instance[browser]["pages"][
                            page
                        ]

                        # check if main instances are present
                        if (
                            page_instance_data["working_status"] == "pending"
                            and self.__browser_instances[browser]
                            and self.__browser_instances[browser]["pages"][page]
                        ):
                            task_list.append(
                                self.__page_scraper_task(
                                    page_instance_data, page, browser
                                )
                            )

                await asyncio.gather(*task_list)

                if self.__pages_hanged_browsers:
                    print("Closing browsers cz a page is hanged")

                    for browser_id in self.__pages_hanged_browsers:
                        if self.__check_if_browser_instance_present(browser_id):
                            await self.__browser_instances[browser_id][
                                "browser"
                            ].disconnect()  # without disconnect browser close hangs
                            await self.__browser_instances[browser_id][
                                "browser"
                            ].close()

                            del self.__browser_instances[browser_id]

                    self.__pages_hanged_browsers = []

                    print("Closed browsers cz a page is hanged")

                self.__urls_are_set_into_page_instances = False

            await asyncio.sleep(int(os.getenv("SLEEP_TIME")))

    async def __page_scraper_task(self, page_instance_data, page_id, browser_id):
        # handle page scrape
        if self.__check_if_browser_and_page_instance_present(browser_id, page_id):
            self.__browser_instances[browser_id]["pages"][page_id][
                "working_status"
            ] = "true"

            done, pending = await asyncio.wait(
                [self.__handle_product_page_scrape(page_instance_data)],
                return_when=asyncio.ALL_COMPLETED,
                timeout=50,
            )

            if pending:
                print("Page hanged...")

                if browser_id not in self.__pages_hanged_browsers:
                    self.__pages_hanged_browsers.append(browser_id)

        # again check cz upper actions can take time & in that time browser or page may be removed
        if self.__check_if_browser_and_page_instance_present(browser_id, page_id):
            self.__browser_instances[browser_id]["pages"][page_id][
                "working_status"
            ] = "false"
            self.__browser_instances[browser_id]["pages"][page_id]["info"] = {}

    def __check_if_browser_and_page_instance_present(
        self, browser_id, page_id, browser_instance=None
    ):
        if not browser_instance:
            browser_instance = self.__browser_instances

        return self.__check_if_browser_instance_present(
            browser_id, browser_instance
        ) and self.__check_if_page_instance_present(
            page_id, browser_instance[browser_id]["pages"]
        )

    def __check_if_browser_instance_present(self, browser_id, browser_instance=None):
        if not browser_instance:
            browser_instance = self.__browser_instances

        return browser_instance[browser_id]

    def __check_if_page_instance_present(
        self, page_id, page_instance=None, browser_id=None
    ):
        if not page_instance:
            page_instance = self.__browser_instances[browser_id]["pages"]

        return page_instance[page_id]

    async def __handle_product_page_scrape(self, page_instance_data):
        page_info = page_instance_data["info"]
        goto_url = page_info["url"]
        page_instance = page_instance_data["page"]

        page_content_compressed = zlib.compress("".encode(), 5)
        page_status = 0

        product_page_id = page_info["other_info"]["_id"]

        print("Getting product page ----- " + goto_url + " -----")

        try:
            page_response = await page_instance.goto(
                goto_url, {"waitUntil": "networkidle2"}
            )
            page_status = page_response.status

            print("Page ----- " + goto_url + " ----- status ----- " + str(page_status))

            if str(page_status) and str(page_status)[0] in [
                "2",
                "3",
            ]:  # check if status code 2xx or 3xx :
                # page_headers = page_response.headers
                page_content = await page_instance.content()

                parsed_content = BeautifulSoup(page_content, features="lxml")

                # try:
                cart_div = parsed_content.find("form", id="addToCart")

                if cart_div:
                    availability = cart_div.find("div", id="availability")

                    outOfStock = cart_div.find("div", id="outOfStock")
                    # print(cart_div)
                    # print(availability)
                    # print(outOfStock)

                    if not availability and not outOfStock:
                        print("Doing advance cart check for " + goto_url)

                        ds = parsed_content.find(
                            "select", id="native_dropdown_selected_size_name"
                        )

                        if ds:
                            await page_instance.click(
                                "select#native_dropdown_selected_size_name"
                            )

                            page_content = await page_instance.content()
                            parsed_content = BeautifulSoup(
                                page_content, features="lxml"
                            )

                            dsi = parsed_content.find("li", id="size_name_0")

                            if dsi:
                                await page_instance.click("li#size_name_0 > a")
                                await page_instance.waitForNavigation(
                                    {"waitUntil": "networkidle2"}
                                )
                            else:
                                page_status = 888

                                print(
                                    "Select elements not found for  "
                                    + product_page["url"]
                                )
                        else:
                            page_status = 888

                            print("Select tag not found for  " + product_page["url"])

                page_content = await page_instance.content()
                page_content_compressed = zlib.compress(page_content.encode(), 5)
            else:
                print("Page Status " + goto_url + " => " + str(page_status))
        except:
            print("Something went wrong with page " + goto_url)

            page_status = 999

        await self.__update_product_page_meta(
            product_page_id, page_content_compressed, page_status
        )

        await self.__update_product_page_scraped_time(product_page_id)

        print("Done product page ----- " + goto_url + " -----")

        await asyncio.sleep(int(os.getenv("SLEEP_TIME")))

    async def __update_product_page_scraped_time(self, product_page_id):
        await self.__amazon_products.update_one(
            {"_id": product_page_id},
            {"$set": {"updated_at.last_scraped_at": now_time()}},
        )

    async def __update_product_page_meta(
        self, product_page_id, compressed_content, status
    ):
        await self.__amazon_products_meta.update_one(
            {"amazon_product_id": str(product_page_id)},
            {
                "$set": {
                    "amazon_product_id": str(product_page_id),
                    "compressed_content": compressed_content,
                    "page_status": status,
                }
            },
            upsert=True,
        )


print("Initiating Process")

ProductScraper()

print("All Done")  # but it won't be called ever
