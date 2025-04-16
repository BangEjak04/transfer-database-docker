def convert_users_data(data, source):
    """Ubah data ke format dict standar: {name, email}"""
    converted = []

    if source == "mysql" or source == "postgres":
        for row in data:
            converted.append({
                "name": row[0],
                "email": row[1]
            })
    elif source == "mongodb":
        for doc in data:
            converted.append({
                "name": doc.get("name"),
                "email": doc.get("email")
            })
    elif source == "cassandra":
        for row in data:
            converted.append({
                "name": row.name,
                "email": row.email
            })
    return converted
