from hobgoblin import element


class TestRegisterVertex1(element.Vertex):
    pass


class TestRegisterVertex2(element.Vertex):
    pass


class TestRegisterEdge1(element.Edge):
    pass


class TestRegisterEdge2(element.Edge):
    pass


class NotAModelShouldNotBeRegistered:
    pass


class TestRegisterVertexProperty1(element.VertexProperty):
    pass


class TestRegisterVertexProperty2(element.VertexProperty):
    pass
