from rethinkdb import r

conn = r.connect()

r.db_create("twitch").run(conn)

def rdb():
    return r.db("twitch")

rdb().table_create("todo").run(conn)
rdb().table_create("error").run(conn)
rdb().table_create("uploads").run(conn)
rdb().table_create("secrets").run(conn)

rdb().table("todo").index_create("status").run(conn)
rdb().table("todo").index_create("item").run(conn)
rdb().table("todo").index_create("queued_for_item").run(conn)
rdb().table("error").index_create("item").run(conn)
rdb().table("error").index_create("queued_for_item").run(conn)
rdb().table("uploads").index_create("completed").run(conn)
rdb().table("uploads").index_create("dir").run(conn)
rdb().table("uploads").index_create("status").run(conn)

print("Done!")
