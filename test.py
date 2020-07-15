# import asyncio
# import time
#
#
# async def foo( n ):
#     await asyncio.sleep(n)  # wait 5s before continuing
#     print(f"n: {n}!")
#     print(time.time())
#     return n
#
#
# def got_result(future):
#     print(f"got the result! {future.result()}")
#
#
# def cc(time):
#     t = asyncio.create_task(foo(time))
#     t.add_done_callback(got_result)
#
#     return t
#
#
# async def main():
#     print(time.time())
#     tasks = [ cc(5), cc(10), cc(2) ]
#     await asyncio.gather(*tasks)
#     print(time.time())
#
#
# asyncio.run(main())

# a = set(['a', 'b', 'c'])
# b = set(['a', 'd'])
#
# print(list(a - b))
# print(b - a)
# print(a | b)
# print(b | a)
# print(a & b)
# print(a ^ b)

# from urllib import parse
# import tldextract
# pu = 'https://www.amazon.com/dp/B0868LNQPR?tag=georiot-us-default-20&th=1&psc=1&ascsubtag=trd-9297144989887961000-20#a'
#
# p = parse.urlparse(pu)
#
# print(p)
# print(parse.urlunparse((p.scheme, p.netloc, p.path, '', '', '')))
# print(p.query)
# print(parse.parse_qs(p.query))
# print(tldextract.extract('am.com.bd/as/a?a').domain)

import asyncio
import random


async def coro(tag):
    print(">", tag)

    time = random.uniform(0.5, 10)
    await asyncio.sleep(time)
    print("<", tag, "time_take", time)
    return tag


loop = asyncio.get_event_loop()


async def ll():
    tasks = [coro(i) for i in range(1, 2)]

    print("Get first result:")

    done, pending = await asyncio.wait(
        tasks, return_when=asyncio.ALL_COMPLETED, timeout=1
    )

    print(done)
    print("======")
    print(pending)

    if done:
        print("done")

    if pending:
        print("has pending")


loop.run_until_complete(ll())


# for task in finished:
#     print(task.result())
# print("unfinished:", len(unfinished))

# print("Get more results in 2 seconds:")
# finished2, unfinished2 = loop.run_until_complete(
#     asyncio.wait(unfinished, timeout=2))

# for task in finished2:
#     print(task.result())
# print("unfinished2:", len(unfinished2))

# print("Get all other results:")
# finished3, unfinished3 = loop.run_until_complete(asyncio.wait(unfinished2))

# for task in finished3:
#     print(task.result())

loop.close()
