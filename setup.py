from distutils.core  import setup

setup(name='REDqueue',
      verseion='0.0.1',
      description='A light weigth queue server in python',
      author='Zeng Ke',
      packages = ['redqueue'],
      scripts = ['redqueue_server.py']
    )
