from time import sleep
from tqdm import tqdm


class TqdmUpTo(tqdm):
    def update_to(self, i):
        self.set_description('Processing...{}'.format(i))
        self.update(1)


def some_proc(func=None):
    for i in range(0, 99):
        if func:
            func(i)
        sleep(1)


with TqdmUpTo(total=100) as t:
    some_proc(func=t.update_to)
