import asyncio

import pynetidx


def cb(id, val):
    print("callback", id, val)


async def subscribe():
    print("Starting subscriber")
    sub = pynetidx.PySubscriber()
    ret = await sub.subscribe(["/test", "/xxx"])
    while True:
        data = await sub.receive(cb)
        print(data)


async def publish():
    print("Starting publisher")
    pub = pynetidx.PyPublisher()
    a = 1.1
    while True:
        ret = await pub.publish({"/test": a, "/xxx": "xxx"})
        await asyncio.sleep(1)
        a += 1


if __name__ == "__main__":

    async def main():
        await asyncio.gather(subscribe(), publish())

    asyncio.run(main())
