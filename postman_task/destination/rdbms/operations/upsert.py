from sqlalchemy.dialects.postgresql import insert

def upsert_wrap(meta):
    def upsert(table, conn, keys, data_iter, meta=meta):
        upsert_args = {"constraint": "unique_key"}
        for data in data_iter:
            data = {k: data[i] for i, k in enumerate(keys)}
            upsert_args["set_"] = data
            insert_stmt = insert(meta.tables[table.name]).values(**data)
            upsert_stmt = insert_stmt.on_conflict_do_update(**upsert_args)
            conn.execute(upsert_stmt)
    return upsert