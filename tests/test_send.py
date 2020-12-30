from shuniu import core

user = "9c27eefa-1ba1-4b11-a4b1-cd97d4771517"
password = "zxcdsa"

api = core.shuniuRPC("http://127.0.0.1:8090", user, password)
api.login()

api.conf["router"] = {
    "worker": "9c27eefa-1ba1-4b11-a4b1-cd97d4771517"
}


for i in range(10):
    print(api.apply_async("worker.host_collect", kwargs={"ip": "192.168.41.1"}))