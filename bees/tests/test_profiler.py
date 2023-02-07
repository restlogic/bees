from __future__ import absolute_import

from unittest import TestCase, mock

from bees import profiler


@profiler.trace('add')
def trace_func(a=5, b=10):
    return a + b


@profiler.trace('foo')
def trace_func_error():
    raise ValueError()


class TestFuncDecorator(TestCase):

    @mock.patch("opentracing.tracer")
    def test_args(self, mock_tracer):
        self.assertEqual(trace_func(10, 20), 30)
        mock_tracer.start_active_span.assert_called_once()

        mock_scope = mock_tracer.start_active_span.return_value
        mock_span = mock_scope.__enter__.return_value.span
        mock_span.set_tag.assert_called()

        expected_calls = [
            mock.call("function.args", str((10, 20))),
            mock.call("function.kwargs", str({})),
            mock.call("function.result", "30"),
        ]

        self.assertIn(expected_calls, mock_span.set_tag.call_args_list)
        mock_scope.__exit__.assert_called_once()

    @mock.patch("opentracing.tracer")
    def test_kwargs(self, mock_tracer):
        self.assertEqual(trace_func(b=5), 10)
        mock_tracer.start_active_span.assert_called_once()

        mock_scope = mock_tracer.start_active_span.return_value
        mock_span = mock_scope.__enter__.return_value.span
        mock_span.set_tag.assert_called()

        expected_calls = [
            mock.call("function.args", str(())),
            mock.call("function.kwargs", str({'b': 5})),
            mock.call("function.result", "10"),
        ]

        self.assertIn(expected_calls, mock_span.set_tag.call_args_list)
        mock_scope.__exit__.assert_called_once()

    @mock.patch("opentracing.tracer")
    def test_with_exception(self, mock_tracer):
        self.assertRaises(ValueError, trace_func_error)
        mock_tracer.start_active_span.assert_called_once()

        mock_scope = mock_tracer.start_active_span.return_value
        mock_span = mock_scope.__enter__.return_value.span
        mock_span.set_tag.assert_called()

        expected_calls = [
            mock.call("exception.type", "ValueError"),
            mock.call("exception.message", ""),
        ]

        self.assertIn(expected_calls, mock_span.set_tag.call_args_list)
        mock_scope.__exit__.assert_called_once()


class FakeTracedCls(object):

    def method1(self, a, b, c=10):
        return a + b + c

    def method2(self, d, e):
        return d - e

    def method3(self, g=10, h=20):
        return g * h

    def _method(self, i):
        return i


@profiler.trace_cls('rpc')
class FakeTrace(FakeTracedCls):
    pass


@profiler.trace_cls('rpc', trace_private=True)
class FakeTracePrivate(FakeTracedCls):
    pass


class FakeTraceStaticBase(FakeTracedCls):
    @staticmethod
    def static_method(arg):
        return arg


@profiler.trace_cls('rpc', trace_static_methods=True)
class FakeTraceStatic(FakeTraceStaticBase):
    pass


@profiler.trace_cls('rpc')
class FakeTraceStaticSkip(FakeTraceStaticBase):
    pass


class FakeTraceClassBase(FakeTracedCls):
    @classmethod
    def class_method(cls, arg):
        return arg


@profiler.trace_cls('rpc', trace_class_methods=True)
class FakeTraceClass(FakeTraceClassBase):
    pass


@profiler.trace_cls('rpc')
class FakeTraceClassSkip(FakeTraceClassBase):
    pass


class TestClsDecorator(TestCase):

    @mock.patch("opentracing.tracer")
    def test_args(self, mock_tracer):
        fake_cls = FakeTrace()
        self.assertEqual(fake_cls.method1(5, 15), 30)
        mock_tracer.start_active_span.assert_called_once()

        mock_scope = mock_tracer.start_active_span.return_value
        mock_span = mock_scope.__enter__.return_value.span
        mock_span.set_tag.assert_called()

        expected_calls = [
            mock.call("function.args", str((fake_cls, 5, 15))),
            mock.call("function.kwargs", str({})),
            mock.call("function.result", "30"),
        ]

        self.assertIn(expected_calls, mock_span.set_tag.call_args_list)
        mock_scope.__exit__.assert_called_once()

    @mock.patch("opentracing.tracer")
    def test_kwargs(self, mock_tracer):
        fake_cls = FakeTrace()
        self.assertEqual(fake_cls.method3(g=5, h=10), 50)
        mock_tracer.start_active_span.assert_called_once()

        mock_scope = mock_tracer.start_active_span.return_value
        mock_span = mock_scope.__enter__.return_value.span
        mock_span.set_tag.assert_called()

        expected_calls = [
            mock.call("function.args", str((fake_cls,))),
            mock.call("function.kwargs", str({'g': 5, 'h': 10})),
            mock.call("function.result", "50"),
        ]

        self.assertIn(expected_calls, mock_span.set_tag.call_args_list)
        mock_scope.__exit__.assert_called_once()

    @mock.patch("opentracing.tracer")
    def test_private(self, mock_tracer):
        fake_cls = FakeTracePrivate()
        self.assertEqual(fake_cls._method(10), 10)
        mock_tracer.start_active_span.assert_called_once()

        mock_scope = mock_tracer.start_active_span.return_value
        mock_span = mock_scope.__enter__.return_value.span
        mock_span.set_tag.assert_called()

        expected_calls = [
            mock.call("function.args", str((fake_cls, 10))),
            mock.call("function.kwargs", str({})),
            mock.call("function.result", "10"),
        ]

        self.assertIn(expected_calls, mock_span.set_tag.call_args_list)
        mock_scope.__exit__.assert_called_once()

    @mock.patch("opentracing.tracer")
    def test_static(self, mock_tracer):
        fake_cls = FakeTraceStatic()
        self.assertEqual(fake_cls.static_method(10), 10)
        mock_tracer.start_active_span.assert_called_once()

        mock_scope = mock_tracer.start_active_span.return_value
        mock_span = mock_scope.__enter__.return_value.span
        mock_span.set_tag.assert_called()

        expected_calls = [
            mock.call("function.args", str((10,))),
            mock.call("function.kwargs", str({})),
            mock.call("function.result", "10"),
        ]

        self.assertIn(expected_calls, mock_span.set_tag.call_args_list)
        mock_scope.__exit__.assert_called_once()

    @mock.patch("opentracing.tracer")
    def test_static_skip(self, mock_tracer):
        fake_cls = FakeTraceStaticSkip()
        self.assertEqual(fake_cls.static_method(10), 10)
        mock_tracer.start_active_span.assert_not_called()

    @mock.patch("opentracing.tracer")
    def test_class(self, mock_tracer):
        self.assertEqual(FakeTraceClass.class_method(10), 10)
        mock_tracer.start_active_span.assert_called_once()

        mock_scope = mock_tracer.start_active_span.return_value
        mock_span = mock_scope.__enter__.return_value.span
        mock_span.set_tag.assert_called()

        expected_calls = [
            mock.call("function.args", str((FakeTraceClass, 10))),
            mock.call("function.kwargs", str({})),
            mock.call("function.result", "10"),
        ]

        self.assertIn(expected_calls, mock_span.set_tag.call_args_list)
        mock_scope.__exit__.assert_called_once()

    @mock.patch("opentracing.tracer")
    def test_class_skip(self, mock_tracer):
        self.assertEqual(FakeTraceClassSkip.class_method(10), 10)
        mock_tracer.start_active_span.assert_not_called()
