from __future__ import absolute_import

import functools
import inspect

from opentracing import global_tracer

from .utils import get_callable_name, get_class_name, getmembers, parse_obj


def trace(name, info=None, hide_args=False, hide_result=False):
    """Trace decorator for functions."""

    if not info:
        info = {}
    else:
        info = info.copy()
    info["function"] = {}

    def decorator(f):

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            info_ = info
            tracer = global_tracer()

            with tracer.start_active_span(operation_name=name) as scope:
                span = scope.span

                if name not in info_["function"]:
                    function_name = get_callable_name(f)
                else:
                    function_name = info_["function"]["name"]
                span.set_tag('function.name', function_name)

                if not hide_args:
                    span.set_tag('function.args', str(parse_obj(args)))
                    span.set_tag('function.kwargs', str(parse_obj(kwargs)))

                try:
                    try:
                        result = f(*args, **kwargs)
                    except TypeError:
                        result = f(*args[1:], **kwargs)
                    # if len(args) and inspect.isclass(args[0]):
                    #     result = f(*args[1:], **kwargs)
                    # else:
                    #     result = f(*args, **kwargs)
                    if not hide_result: 
                        span.set_tag('function.result', repr(result))
                    return result
                except Exception as e:
                    span.set_tag('exception.type', get_class_name(e))
                    span.set_tag('exception.message', str(e))
                    raise

        return wrapper

    return decorator


def trace_cls(name, info=None, hide_args=False, hide_result=False, trace_private=False, trace_static_methods=False,
              trace_class_methods=False):
    """Trace decorator for instances of class."""

    def trace_checker(attr_name, to_be_wrapped):
        if attr_name.startswith("__"):
            return False

        if not trace_private and attr_name.startswith("_"):
            return False

        if isinstance(to_be_wrapped, staticmethod):
            if not trace_static_methods:
                return False
            return True

        if isinstance(to_be_wrapped, classmethod):
            if not trace_class_methods:
                return False
            return True

        return True

    def decorator(cls):
        clss = cls if inspect.isclass(cls) else cls.__class__
        mro_dicts = [c.__dict__ for c in inspect.getmro(clss)]

        traceable_attrs = [] 
        traceable_wrappers = [] 

        for attr_name, attr in getmembers(cls):
            if not (inspect.ismethod(attr) or inspect.isfunction(attr)): 
                continue
            wrapped_obj = None
            for cls_dict in mro_dicts: 
                if attr_name in cls_dict: 
                    wrapped_obj = cls_dict[attr_name] 
                    break

            should_wrap = trace_checker(attr_name, wrapped_obj)
            if not should_wrap:
                continue
            if isinstance(wrapped_obj, staticmethod):
                wrapper = staticmethod
            elif isinstance(wrapped_obj, classmethod):
                wrapper = classmethod
            else:
                wrapper = None

            traceable_attrs.append((attr_name, attr)) 
            traceable_wrappers.append(wrapper) 

        for i, (attr_name, attr) in enumerate(traceable_attrs):
            wrapped_method = trace(name, info=info, hide_args=hide_args, 
                                   hide_result=hide_result)(attr) 
            wrapper = traceable_wrappers[i]
            if wrapper is not None:
                wrapped_method = wrapper(wrapped_method)

            setattr(cls, attr_name, wrapped_method)

        return cls

    return decorator


def _ensure_no_multiple_traced(traceable_attrs):
    for attr_name, attr in traceable_attrs:
        traced_times = getattr(attr, "__traced__", 0)
        if traced_times:
            raise ValueError("Can not apply new trace on top of "
                             "previously traced attribute '%s' since"
                             " it has been traced %s times previously"
                             % (attr_name, traced_times))


class TracedMeta(type):
    """Metaclass to comfortably trace all children of a specific class.

    Possible usage:

    >>>  @six.add_metaclass(profiler.TracedMeta)
    >>>  class RpcManagerClass(object):
    >>>      __trace_args__ = {'name': 'rpc',
    >>>                        'info': None,
    >>>                        'hide_args': False,
    >>>                        'hide_result': True,
    >>>                        'trace_private': False}
    >>>
    >>>      def my_method(self, some_args):
    >>>          pass
    >>>
    >>>      def my_method2(self, some_arg1, some_arg2, kw=None, kw2=None)
    >>>          pass

    Adding of this metaclass requires to set __trace_args__ attribute to the
    class we want to modify. __trace_args__ is the dictionary with one
    mandatory key included - "name", that will define name of action to be
    traced - E.g. wsgi, rpc, db, etc...
    """

    def __init__(cls, cls_name, bases, attrs):
        super(TracedMeta, cls).__init__(cls_name, bases, attrs)

        trace_args = dict(getattr(cls, "__trace_args__", {}))
        trace_private = trace_args.pop("trace_private", False)
        if "name" not in trace_args:
            raise TypeError("Please specify __trace_args__ class level "
                            "dictionary attribute with mandatory 'name' key - "
                            "e.g. __trace_args__ = {'name': 'rpc'}")

        traceable_attrs = []
        for attr_name, attr_value in attrs.items():
            if not (inspect.ismethod(attr_value) or inspect.isfunction(attr_value)):
                continue

            if attr_name.startswith("__"):
                continue

            if not trace_private and attr_name.startswith("_"):
                continue
            traceable_attrs.append((attr_name, attr_value))

        for attr_name, attr_value in traceable_attrs:
            setattr(cls, attr_name, trace(**trace_args)(getattr(cls, attr_name)))
