import json
from types import NoneType

Types = {
    int: "int",
    float: "float",
    list: "list",
    tuple: "tuple",
    dict: "dict",
    bool: "bool",
    str: "str",
    NoneType: "null",
    "*": "*",
}


def typename(value) -> str:
    """Возвращает строковый тип значения."""
    return Types.get(type(value), "*")


class Templater:
    """
    Загружает шаблоны и позволяет получать конкретный шаблон 
    по пути и типу события: in/out/inout.
    """

    def __init__(self, filename: str):
        self.templates = {}
        with open(filename, "r") as f:
            templates = json.load(f)

        # Разворачиваем ключи вида "dict.list.*" в дерево словарей
        for dotted_key, tmpl in templates.items():
            keys = dotted_key.split(".")
            leaf = keys[-1]
            node = self._ensure_path(keys[:-1])
            node[leaf] = tmpl

        self.default = self.templates.get("*", {})

    def _ensure_path(self, keys: list):
        node = self.templates
        for k in keys:
            node = node.setdefault(k, {})
        return node

    def _lookup_template(self, path: list, event_key: str, may_skip: bool):
        node = self.templates

        for key in path:
            # Если это не dict — значит путь привёл в лист, дальше идти нельзя
            if not isinstance(node, dict):
                node = self.default
                break

            if key in node:
                node = node[key]
            elif "*" in node:
                node = node["*"]
            else:
                node = None
                break

        if node is None:
            node = self.default

        # Если снова попали в лист — нельзя искать .get()
        if not isinstance(node, dict):
            node = self.default

        tmpl = node.get(event_key)

        if tmpl is None:
            if may_skip:
                return None
            return self.default.get(event_key, "{host}.{name} = ?\n")

        tail = node.get("*")
        if isinstance(tail, str):
            tmpl += tail

        return tmpl

    def _format(self, path: list, value, event: str):
        may_skip = event.startswith("*")
        if may_skip:
            event = event[1:]

        type_key = typename(value)
        search_key = f"{event}_{type_key}" if event else type_key

        tmpl = self._lookup_template(path, search_key, may_skip)
        if not tmpl:
            return None

        name = path[-1] if path else ""
        host = path[-2] if len(path) > 1 else ""

        return tmpl.format(host=host, name=name, value=value)

    def In(self, path: list, value):
        return self._format(path, value, "in")

    def Out(self, path: list, value):
        return self._format(path, value, "out")

    def InOut(self, path: list, value):
        return self._format(path, value, "")

    def Next(self, path: list, value):
        return self._format(path, value, "*next_in")


class TreeProcessing:
    """
    Рекурсивно обходит структуру (dict/list) и генерирует строки 
    по шаблонам.
    """

    def __init__(self, tname: str = "templates.json"):
        self.templater = Templater(tname)

    def _process_item(self, key: str, value, path: list, next_host) -> list:
        path.append(str(key))  # Преобразуем ключ в строку для единообразия

        # Обрабатываем ВСЕ элементы рекурсивно
        if isinstance(value, (dict, list | tuple)):
            lines = self.convert(value, path)
        else:
            lines = [self.templater.InOut(path, value)]

        # next_host лучше привести к типу
        if next_host is not False:
            nxt = self.templater.Next(path, typename(next_host))
            if nxt:
                lines.append(nxt)

        path.pop()
        return lines

    def convert(self, data: dict | list, path: list | None = None) -> list:
        if path is None:
            path = []

        # Вставляем начало
        lines = [self.templater.In(path, data)]

        # Если список пустой — никаких элементов не обрабатываем
        if isinstance(data, list | tuple):
            if not data:  # список пустой
                # никаких next, просто завершаем
                lines.append(self.templater.Out(path, data))
                return lines

            # обычная обработка списка
            for i, value in enumerate(data):
                next_host = data[i + 1] if i + 1 < len(data) else False
                lines.extend(self._process_item(i, value, path, next_host))

        elif isinstance(data, dict):
            keys = list(data.keys())
            for i, key in enumerate(keys):
                next_host = data[keys[i + 1]] if i + 1 < len(keys) else False
                lines.extend(self._process_item(
                    key, data[key], path, next_host))

        else:
            raise TypeError("convert() ожидает dict или list")

        lines.append(self.templater.Out(path, data))
        return lines


if __name__ == "__main__":
    testdict = json.loads(open('templates/example.json', 'r').read())
    converter = TreeProcessing('templates/template.json')
    lst = converter.convert(testdict)
    print("".join(lst))
