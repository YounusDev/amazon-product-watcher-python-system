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
