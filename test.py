import asyncio

import pynetidx


def cb(id, val):
    print(id, val)


async def subscribe():
    sub = pynetidx.PySubscriber()
    ret = await sub.subscribe(["/test"])
    print(ret)


async def publish():
    pub = pynetidx.PyPublisher()
    ret = await pub.publish({"/test": 123})
    print(ret)


if __name__ == "__main__":

    async def main():
        await asyncio.gather(subscribe(), publish())

    asyncio.run(main())
