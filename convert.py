def flatten_dict_with_template(data, templates={}, host=""):
    lines = []

    for key, value in data.items():
        current_host = f"{host}.{key}" if host else key

        # Словарь
        if isinstance(value, dict):
            lines.extend(flatten_dict_with_template(
                value, templates, host=current_host))
            continue

        # Список
        if isinstance(value, list):
            tpl = templates.get("list", "{host}.{name}[{index}] = {value}")
            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    # если элемент - dict -> рекурсивно
                    lines.extend(
                        flatten_dict_with_template(
                            item, templates, host=f"{current_host}[{idx}]"
                        )
                    )
                else:
                    val = item
                    line = tpl.format(
                        host=host,
                        name=key,
                        index=idx,
                        value=val
                    )
                    lines.append(line)
            continue

        # Примитивные типы
        if value is None:
            tpl = templates.get("none", "{host}.{name} = null")
            vtype = "none"
        elif isinstance(value, bool):
            tpl = templates.get("bool", "{host}.{name} = {value}")
            vtype = "bool"
        elif isinstance(value, int):
            tpl = templates.get("int", "{host}.{name} = {value}")
            vtype = "int"
        elif isinstance(value, float):
            tpl = templates.get("float", "{host}.{name} = {value}")
            vtype = "float"
        else:
            tpl = templates.get("str", "{host}.{name} = {value}")
            vtype = "str"

        line = tpl.format(
            host=host,
            name=key,
            value=value,
            type=vtype
        )
        lines.append(line)

    return lines
