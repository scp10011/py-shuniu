from shuniu import core

user = "9c27eefa-1ba1-4b11-a4b1-cd97d4771517"
password = "zxcdsa"
rpc_server = f"http://{user}:{password}@127.0.0.1:8090"

app = core.Shuniu("worker", rpc_server)


@app.task(name="worker.host_collect", bind=True)
def host_collect(self):
    print(123)


app.start()
