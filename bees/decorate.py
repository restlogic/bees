import functools
import inspect

import six
from opentracing import global_tracer

from .utils import get_callable_name, get_own_members


def _ensure_no_multiple_traced(traceable_attrs):
    for attr_name, attr in traceable_attrs:
        traced_times = getattr(attr, "__traced__", 0)
        if traced_times:
            raise ValueError("Can not apply new trace on top of "
                             "previously traced attribute '%s' since"
                             " it has been traced %s times previously"
                             % (attr_name, traced_times))


def trace(name, info=None, hide_args=False, hide_result=False,
          allow_multiple_trace=True):
    """
    Trace decorator for functions.
    :param name: The name of action
    :param info: Dictionary with extra trace information. For example in wsgi
     it can be url, in rpc - message or in db sql -
    :param hide_args: Don't push to trace info args and kwargs. Quite useful
    if you have some info in args that you wont to share,
    :param hide_result: Boolean value to hide/show function result in trace.
    :param allow_multiple_trace: allow the wrapped function be traced mutiple
    times
    """
    if not info:
        info = {}
    else:
        info = info.copy()

    def decorator(func):
        trace_times = getattr(func, "__traced__", 0)
        if not allow_multiple_trace and trace_times:
            raise ValueError("Function '%s' has already"
                             " been traced %s times" % (func, trace_times))
        try:
            func.__traced__ = trace_times + 1
        except AttributeError:
            # Tries to work around the following:
            #
            # AttributeError: 'instancemethod' object has no
            # attribute '__traced__'
            try:
                func.im_func.__traced__ = trace_times + 1
            except AttributeError:
                pass

        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            # get the function name and args kwargs for the function
            info_ = info

            # if not hide args, put the args info in the args.
            if not hide_args:
                args_repr = []
                for a in args:
                    if hasattr(a, "__dict__"):
                        args_repr.append(get_callable_name(a))
                        args_repr.extend(
                            "{%s}={%s}" % (repr(attrname), repr(attr)) for attrname, attr in a.__dict__.items())
                    else:
                        args_repr.append(repr(a))

                # args_repr = [repr(a) for a in args]
                kwargs_repr = ["{%s}={%s}" % (repr(k), repr(v)) for k, v in kwargs.items()]

                info_["signature"] = ",".join(args_repr + kwargs_repr)
                info_["call_name"] = get_callable_name(func)

            with global_tracer().start_active_span(operation_name=name) as scope:

                # put the args and kwargs into the tags
                for k, v in info_.items():
                    scope.span.set_tag(k, v)

                result = func(*args, **kwargs)

                # if not hide result
                if not hide_result:
                    scope.span.log_kv({"result": repr(result)})
                return result

        return wrapper

    return decorator


def trace_cls(name, info=None, hide_args=False, hide_result=False,
              trace_private=False, allow_multiple_trace=True,
              trace_class_methods=False, trace_static_methods=False):
    """
    Trace decorator for instances of class.

    Very useful if you would like to add trace point on existing method:
    :param name: The name of action. E.g. wsgi, rpc, db, etc..
    :param info: Dictionary with extra trace information. For example in wigi
                 it can be url, in rpc - message
    :param hide_args:  Don't push to trace info args and kwargs. Quite useful
                       if you have some info in args that you wont to share,
    :param hide_result: Boolean value to hide/show function result in trace.
    :param trace_private: Trace methods that starts with "_". it wont trace
                          method that starts "__" even if it is turned on
    :param allow_multiple_trace: If wrapped attributes have already been
                                 traced either allow the new trace to occur
                                 or raise a value error denoting that multiple
                                 tracing is not allowed(by default allow)
    :param trace_class_methods:  Trace classmethods. This may be prone to
                                 issues so careful usage is recommend(this
                                 is also why this defaults to false)
    :param trace_static_methods: Trace staticmethod. This may be prone to
                                  issues so careful usage is recommended(this
                                  is also why this defaults to false)
    :return:
    """

    def trace_checker(attr_name, to_be_wrapped):
        if (attr_name.startswith("__")):
            # Never trace really private methods.
            return (False, None)
        if not trace_private and attr_name.startswith("_"):
            return (False, None)
        if isinstance(to_be_wrapped, staticmethod):
            if not trace_static_methods:
                return (False, None)
            return (True, staticmethod)
        if isinstance(to_be_wrapped, classmethod):
            if not trace_class_methods:
                return (False, None)
            return (True, classmethod)
        return (True, None)

    def decorator(cls):
        # get the classs name for the cls
        clss = cls if inspect.isclass(cls) else cls.__class__
        mro_dicts = [c.__dict__ for c in inspect.getmro(clss)]
        traceable_attrs = []
        traceable_wrappers = []
        for attr_name, attr in get_own_members(cls):
            if not (inspect.ismethod(attr) or inspect.isfunction(attr)):
                continue
            wrapped_obj = None
            for cls_dict in mro_dicts:
                if attr_name in cls_dict:
                    wrapped_obj = cls_dict[attr_name]
                    break
            should_wrap, wrapper = trace_checker(attr_name, wrapped_obj)
            if not should_wrap:
                continue
            traceable_attrs.append((attr_name, attr))
            traceable_wrappers.append(wrapper)
        if not allow_multiple_trace:
            # Check before doing any other further work (so we don't
            # halfway trace this class).
            _ensure_no_multiple_traced(traceable_attrs)
        for i, (attr_name, attr) in enumerate(traceable_attrs):
            wrapped_method = trace(name, info=info, hide_args=hide_args,
                                   hide_result=hide_result)(attr)
            wrapper = traceable_wrappers[i]
            if wrapper is not None:
                wrapped_method = wrapper(wrapped_method)
            setattr(cls, attr_name, wrapped_method)
        return cls

    return decorator


class TraceMeta(type):
    """ Metaclass to comfortably trace all children of a specific class.


    >>>  @six.add_metaclass(decorate.TracedMeta)
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
    traced -E.g wsgi, rpc
    """

    def __init__(cls, cls_name, bases, attrs):
        super(TraceMeta, cls).__init__(cls_name, bases, attrs)

        trace_args = dict(getattr(cls, "__trace_args__", {}))
        trace_private = trace_args.pop("trace_private", False)
        allow_multiple_trace = trace_args.pop("allow_multiple_trace", True)
        if "name" not in trace_args:
            raise TypeError("Please specify __trace_args__ class level "
                            "dictionary attribute with mandaotory 'name' key - "
                            "e.g. __trace_args__ ={'name':'rpc'}")

        traceable_attrs = []
        for attr_name, attr_value in attrs.items():
            if not (inspect.ismethod(attr_value)
                    or inspect.isfunction(attr_value)):
                continue
            if attr_name.startswith("__"):
                continue
            if not trace_private and attr_name.startswith("_"):
                continue
            traceable_attrs.append((attr_name, attr_value))
        if not allow_multiple_trace:
            _ensure_no_multiple_traced(traceable_attrs)
        for attr_name, attr_value in traceable_attrs:
            setattr(cls, attr_name, trace(**trace_args)(getattr(cls,
                                                                attr_name)))
