import json


class TreeProcessing:
    templates = json.loads(open('templates.json', 'r').read())

    def in_out(self, host, type, template="default"):
        templates = self.templates.get(template, {})
        if type is None:
            tpl = templates.get("null", "########")
            vtype = "null"
        elif type == 'in_dict':
            tpl = templates.get("in_dict", "{")
            vtype = "in_dict"
        elif type == 'out_dict':
            tpl = templates.get("out_dict", "}")
            vtype = "out_dict"
        elif type == "in_list":
            tpl = templates.get("in_list", "[")
            vtype = "in_list"
        elif type == "out_list":
            tpl = templates.get("out_list", "]")
            vtype = "out_list"
        else:
            tpl = templates.get("root", "{host}.")
            vtype = "root"

        line = tpl.format(
            host=host,
            type=vtype
        )
        return line

    def dict_to_text(self, data, template="default", host="", func = None):
        if func is None:
            func = self.value_to_text

        lines = []

        lines.append(self.in_out(host, 'in_dict'))
        for key, value in data.items():
            current_host = self.in_out(host, '') + key if host else key

            # Словарь
            if isinstance(value, dict):
                lines.extend(self.dict_to_text(
                    value, template=template, host=current_host, func=func))
                continue

            # Список
            if isinstance(value, list) or isinstance(value, tuple):
                lines.append(self.in_out(host, 'in_list'))
                for idx, item in enumerate(value):
                    if isinstance(item, dict):
                        # если элемент - dict -> рекурсивно
                        lines.extend(
                            self.dict_to_text(
                                item, templates=template, host=f"{current_host}[{idx}]", func=func
                            )
                        )
                    else:
                        val = item
                        line = func(host=current_host, name=idx,
                                    value=val, template=template)
                        lines.append(line)
                lines.append(self.in_out(host, 'out_list'))
                continue

            line = func(host=host, name=key,
                        value=value, template=template)
            lines.append(line)
        lines.append(self.in_out(host, 'out_dict'))
        return lines

    def value_to_text(self, host: str, name: str, value, template="default"):
        templates = self.templates.get(template, {})
        # Примитивные типы
        if value is None:
            tpl = templates.get("null", "{host}.{name} = null")
            vtype = "null"
        elif isinstance(value, bool):
            tpl = templates.get("bool", "{host}.{name} = {value}")
            vtype = "bool"
        elif isinstance(value, int):
            tpl = templates.get("int", "{host}.{name} = {value}")
            vtype = "int"
        elif isinstance(value, list):
            tpl = templates.get("list", "{host}.{name} = {value}")
            vtype = "list"
        elif isinstance(value, float):
            tpl = templates.get("float", "{host}.{name} = {value}")
            vtype = "float"
        else:
            tpl = templates.get("str", "{host}.{name} = {value}")
            vtype = "str"

        line = tpl.format(
            host=host,
            name=name,
            value=value,
            type=vtype
        )
        return line
