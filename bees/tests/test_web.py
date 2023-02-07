from __future__ import absolute_import

from unittest import TestCase, mock

from bees import web


class TestWeb(TestCase):

    def tearDown(self) -> None:
        web._DISABLED = None

    def test_disabled(self):
        web.enable()
        self.assertFalse(web._DISABLED)

        web.disable()
        self.assertTrue(web._DISABLED)

    def test_factory(self):
        mock_app = mock.MagicMock()
        local_conf = {"enabled": True}

        factory = web.WsgiMiddleware.factory(None, **local_conf)
        wsgi = factory(mock_app)

        self.assertEqual(wsgi.application, mock_app)
        self.assertEqual(wsgi.name, "wsgi")
        self.assertTrue(wsgi.enabled)

    @mock.patch("opentracing.tracer")
    def test_middleware_disable(self, mock_tracer):
        request = mock.MagicMock()
        request.headers = mock.MagicMock()
        request.get_response.return_value = "Whoops!"

        middleware = web.WsgiMiddleware("app")
        self.assertEqual(middleware(request), "Whoops!")
        mock_tracer.extract.assert_not_called()

    @mock.patch("opentracing.tracer")
    def test_middleware_enable(self, mock_tracer):
        request = mock.MagicMock()
        request.headers = mock.MagicMock()
        request.get_response.return_value = "Catch!"

        middleware = web.WsgiMiddleware("app", enabled=True)
        self.assertEqual(middleware(request), "Catch!")
        mock_tracer.extract.assert_called_once()
        mock_tracer.start_active_span.assert_called_once()
