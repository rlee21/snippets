
if __name__ == '__main__':
    import requests
    import os
    from setup_ex.__version__ import version

    token = os.environ['PACKAGECLOUD_TOKEN']
    packages = requests.get('https://{}:@packagecloud.io/api/v1/repos/avvo/data/packages.json'.format(token))
    push_package = 'setup_ex-{}-py2.py3-none-any.whl'.format(version)

    if not os.path.isfile(os.path.join('dist', push_package)):
        raise IOError('Python dist package "{}" does not exist'.format(push_package))

    for p in packages.json():
        if p['filename'] == push_package:
            requests.delete('https://{}:@packagecloud.io/api/v1/repos/avvo/data/python/wheel/{}'.format(token, push_package))

