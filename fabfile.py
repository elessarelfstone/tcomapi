from fabric.api import local, run
from fabric.api import env, cd

env.hosts = ['dataeng@10.8.36.61']


def hello():
    run('echo Hello!!')

