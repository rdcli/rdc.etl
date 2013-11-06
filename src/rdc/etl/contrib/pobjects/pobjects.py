#! /usr/bin/python2.4
#
# pobject.py: @override, @final etc. decorator support in Python2.4
# by pts@fazekas.hu at Wed Jul  4 10:09:45 CEST 2012
#
# pobjects works in Python 2.4, 2.5, 2.6 and 2.7.
#
# Do `import pobjects_builtins' to do a `from pobjects import *' for all
# modules.
#
# TODO(pts): Add decorators to properties / descriptors.
# !! Get rid of _UnwrapFunction -- for classmethod?
# !! Add tests for @classmethod etc.

"""Module implementing method restrictions (@abstract, @final) etc.

Usage: from pobjects import *
"""

import re
import sys
import types

__all__ = ('final', 'finalim', 'nosuper', 'abstract', 'override')


class AbstractMethodError(Exception):
  """Raised when an abstract method is called."""


class BadClass(Exception):
  """Raised when a class cannot be created."""


class BadInstantiation(Exception):
  """Raised when a class cannot be instantiated."""


class BadDecoration(Exception):
  """Raised when a decorator cannot be applied."""


def _ParseClassNameList(spec, f_globals, f_builtins):
  if not isinstance(spec, str): raise TypeError
  classes = []
  for spec_item in spec.split(','):
    path = spec_item.strip().split('.')
    # TODO(pts): More detailed error reporting.
    if '' in path: return None
    path_start = path.pop(0)
    if path_start in f_globals:
      value = f_globals[path_start]
    elif path_start in f_builtins:
      value = f_builtins[path_start]
    else:
      # TODO(pts): More detailed error reporting.
      return None
    for path_item in path:
      # TODO(pts): More detailed error reporting.
      if not hasattr(value, path_item): return None
      value = getattr(value, path_item)
    classes.append(value)
  # TODO(pts): More detailed error reporting.
  if not classes: return None
  return classes


def _Self(value):
  """Helper identity function."""
  return value


def _UnwrapFunction(fwrap):
  """Unwrap function from function, classmethod or staticmethod.
  
  Args:
    fwrap: A function, classmethod or staticmethod.
  Returns:
    (function, wrapper), where `function' is the function unwrapped from
    fwrap, and wrapper is a callable which can be called on function to
    yield something equivalent to fwrap. The value (None, None) is returned
    if fwrap is is not of the right type.
  """
  # !! add a unit test
  if isinstance(fwrap, types.FunctionType):
    return fwrap, _Self
  elif isinstance(fwrap, classmethod):
    return fwrap.__get__(0).im_func, classmethod
  elif isinstance(fwrap, staticmethod):
    return fwrap.__get__(0), staticmethod
  else:
    return None, None


def _UnwrapFunctionOrMethod(fwrap):
  """Unwrap function from function, method, classmethod or staticmethod.
  
  Args:
    fwrap: A function, method, classmethod or staticmethod.
  Returns:
    function or None.
  """
  # !! add a unit test
  if isinstance(fwrap, (classmethod, staticmethod)):
    fwrap = fwrap.__get__(0)
  if isinstance(fwrap, types.MethodType):
    fwrap = fwrap.im_func
  if isinstance(fwrap, types.FunctionType):
    return fwrap
  else:
    return None


class _OldStyleClass:
  pass


class _MetaClassProxy(object):
  def __init__(self, orig_metaclass):
    self.orig_metaclass = orig_metaclass
    # List of dicts describing decorators applying to methods in this class,
    # see pobject_decorator below.
    self.decorators = []

  def __call__(self, class_name, bases, dict_obj):
    decorator_names = set([decorator['decorator_name']
                           for decorator in self.decorators])
    if not decorator_names or (
        isinstance(self.orig_metaclass, type) and
        issubclass(self.orig_metaclass, _PObjectMeta)):
      return self.orig_metaclass(class_name, bases, dict_obj)
    if ('final' not in decorator_names and 'finalim' not in decorator_names and
        'abstract' not in decorator_names):
      CheckDecorators(class_name, bases, dict_obj)
      if not [1 for base in bases
              if not isinstance(base, type(_OldStyleClass))]:
        # Avoid `TypeError: Error when calling the metaclass bases;
        # a new-style class can't have only classic bases'.
        bases += (object,)
      return self.orig_metaclass(class_name, bases, dict_obj)
    # We have @final and/or @finalim among the decorators we want to apply.
    # Since these have affect on subclasses, we have to set up _PObjectMeta
    # as the metaclass in order to get CheckDecorators called when a
    # subclass is created.
    error_prefix = '; '.join([
        'decorator @%s in file %r, line %s cannot be applied to %s' %
        (decorator['decorator_name'], decorator['file_name'],
        decorator['line_number'], decorator['full_method_name'])
        for decorator in self.decorators]).capitalize()
    full_class_name = '%s.%s' % (dict_obj['__module__'], class_name)
    if (not isinstance(self.orig_metaclass, type) or
        not issubclass(_PObjectMeta, self.orig_metaclass)):
      # TODO(pts): Lift this restriction.
      raise BadDecoration(
          '%s; because the specified %s.__metaclass__ (%s) is not a subclass '
          'of _PObjectMeta.' %
         (error_prefix, full_class_name, self.orig_metaclass))
    # This for loop is to give a better error message than:
    # TypeError: Error when calling the metaclass bases:: metaclass
    # conflict: the metaclass of a derived class must be a (non-strict)
    # subclass of the metaclasses of all its bases
    #bases = (type(re.compile('a').scanner('abc')),)
    old_base_count = 0
    for base in bases:
      if isinstance(base, type(_OldStyleClass)):
        old_base_count += 1
      elif not isinstance(base, type):
        # TODO(pts): exactly how to trigger this
        # Counterexamples: base == float; base == str; regexp match.
        # Examples: Exception.
        raise BadDecoration(
            '%s; because base class %s is not a pure class.' %
            (error_prefix, base))
      elif not issubclass(_PObjectMeta, type(base)):
        raise BadDecoration(
            '%s; because base class %s has metaclass %s, '
            'which conflicts with _PObjectMeta.' %
            (error_prefix, base, type(base)))
    if old_base_count == len(bases):
      # Avoid `TypeError: Error when calling the metaclass bases;
      # a new-style class can't have only classic bases'.
      # TODO(pts): Test this with exceptions.
      bases += (object,)
    return _PObjectMeta(class_name, bases, dict_obj)


def pobject_decorator(decorator):
  #print 'MMM', decorator.func_name
  def ApplyDecorator(fwrap):
    """Applies decorator to function in f (function or method)."""
    function, wrapper = _UnwrapFunction(fwrap)
    if not isinstance(function, types.FunctionType):
      raise BadDecoration(
          'Decorator @%s cannot be applied to non-function %r.' %
          (decorator.func_name, function))
    f = sys._getframe().f_back
    if '__module__' not in f.f_locals:
      raise BadDecoration(
          'decorator @%s cannot be applied to function %s in %s, '
          'because the latter is not a class definition.' %
          (decorator.func_name, function.func_name, f.f_code.co_name))
    module_name = f.f_locals['__module__']
    full_method_name = '%s.%s.%s' % (
        module_name, f.f_code.co_name, function.func_name)
    # `type' below silently upgrades an old-style class to a new-style class.
    metaclass = f.f_locals.get('__metaclass__') or type
    if not isinstance(metaclass, _MetaClassProxy):
      # TODO(pts): Document that this doesn't work if __metaclass__ is
      # assigned after the first decorated method.
      f.f_locals['__metaclass__'] = metaclass = _MetaClassProxy(metaclass)
    metaclass.decorators.append({
        'decorator_name': decorator.func_name,
        'full_method_name': full_method_name,
        'file_name': f.f_code.co_filename,
        'line_number': f.f_lineno,
    })
    decorated_function = decorator(
        function=function, full_method_name=full_method_name)
    #print (decorator.func_name, decorated_function, function, wrapper)
    if decorated_function is function:
      return fwrap  # The wrapped function, classmethod or staticmethod.
    else:
      return wrapper(decorated_function)
  return ApplyDecorator


def _GenerateBothAbstractAndFinal(full_method_name):
  return 'Decorators @abstract and @final cannot be both applied to %s.' % (
      full_method_name)


def _GenerateBothAbstractAndFinalim(full_method_name):
  return 'Decorators @abstract and @finalim cannot be both applied to %s.' % (
      full_method_name)


def _AbstractFunction(self, *args, **kwargs):
  # _func_name and _full_method_name are
  # inserted to our func_globals by the @abstract decorator.
  if not isinstance(self, type):
    self = type(self)
  this_method_name = '%s.%s.%s' % (
      self.__module__, self.__name__, _func_name)
  if this_method_name == _full_method_name:
    raise AbstractMethodError(
        'Abstract method %s called.' % this_method_name)
  raise AbstractMethodError(
      'Abstract method %s called as %s' %
      (_full_method_name, this_method_name))


@pobject_decorator
def abstract(function, full_method_name):
  assert type(function) == types.FunctionType

  if getattr(function, '_is_final', None):
    raise BadDecoration(_GenerateBothAbstractAndFinal(full_method_name))

  # We'd lose ._is_finalim on AbstractFunction anyway.
  if getattr(function, '_is_finalim', None):
    raise BadDecoration(_GenerateBothAbstractAndFinalim(full_method_name))

  if function.func_name == '__init__':
    function._is_abstract = True
    return function

  # TODO(pts0: Copy __doc__ etc.
  f = type(_AbstractFunction)(
      _AbstractFunction.func_code,
      {'__builtins__': _AbstractFunction.func_globals['__builtins__'],
       'AbstractMethodError': AbstractMethodError,
       '_full_method_name': full_method_name,
       '_func_name': function.func_name},
      _AbstractFunction.func_defaults,
      _AbstractFunction.func_closure)
  f._is_abstract = True
  return f


@pobject_decorator
def nosuper(function, full_method_name):
  assert type(function) == types.FunctionType
  function._is_nosuper = True
  return function


@pobject_decorator
def final(function, full_method_name):
  assert type(function) == types.FunctionType

  if getattr(function, '_is_abstract', None):
    raise BadDecoration(_GenerateBothAbstractAndFinal(full_method_name))

  function._is_final = True
  return function


@pobject_decorator
def finalim(function, full_method_name):
  """Declare that the method cannot be overriden with instance methods."""
  assert type(function) == types.FunctionType

  function._is_finalim = True
  return function


@pobject_decorator
def override(function, full_method_name):
  assert type(function) == types.FunctionType
  function._is_override = True
  return function


def _DumpBaseClassList(bases):
  if len(bases) == 1:
    return 'base class %s.%s' % (bases[0].__module__, bases[0].__name__)
  else:
    return 'base classes ' + ', '.join(
        ['%s.%s' % (base.__module__, base.__name__) for base in bases])


def _DumpMethodList(method_names):
  if len(method_names) > 1:
    return 'methods ' + ', '.join(sorted(method_names))
  else:
    return 'method ' + iter(method_names).next()


def _AbstractInit(self, *args, **kwargs):
  # _abstract_method_fullnames, _for_super_class, _has_orig_init, _orig_init
  # in our func_globals was set up by _PObjectMeta.
  if _for_super_class is not type(self):
    # TODO(pts): Avoid this runtime overhead. (Is it possible?)
    if _has_orig_init:
      return _orig_init(self, *args, **kwargs)
    return super(_for_super_class, self).__init__(*args, **kwargs)
  if (len(_abstract_method_fullnames) == 1 and
      iter(_abstract_method_fullnames).next().endswith('.__init__')):
    raise BadInstantiation('Cannot instantiate abstract class %s.%s' %
                           (type(self).__module__, type(self).__name__))
  raise BadInstantiation(
      'Cannot instantiate abstract class %s.%s because '
      'it has @abstract %s' %
      (type(self).__module__, type(self).__name__,
       _DumpMethodList(_abstract_method_fullnames)))


def CheckDecorators(class_name, bases, dict_obj):
  """Raise BadClass if the new class is inconsistent with some decorators.
  
  May make modifications to dict_obj. 
  """
  problems = []
  module = dict_obj['__module__']
  # Maps method names to '<basemodule>.<baseclass>.<method>'s.
  abstract_methods = {}
  for base in bases:
    for name in sorted(dir(base)):
      function = _UnwrapFunctionOrMethod(getattr(base, name))
      if getattr(function, '_is_abstract', None):
        abstract_methods.setdefault(name, []).append(function._full_name)
  has_abstract_method_in_bases = bool(abstract_methods)
  abstract_methods.pop('__init__', None)
  for name in sorted(dict_obj):
    function, _ = _UnwrapFunction(dict_obj[name])
    if isinstance(function, types.FunctionType):
      if (getattr(function, '_is_abstract', None) or
          getattr(function, '_is_final', None) or
          getattr(function, '_is_finalim', None)):
        function._full_name = '%s.%s.%s' % (module, class_name, name)
      if getattr(function, '_is_abstract', None):
        abstract_methods.setdefault(name, []).append(function._full_name)
      else:
        abstract_methods.pop(name, None)
      if getattr(function, '_is_nosuper', None):
        bases_with_name = [base for base in bases if hasattr(base, name)]
        if bases_with_name:
          # Unfortunately, we don't get the method definition line in the
          # traceback. TODO(pts): Somehow forge it.
          problems.append('@nosuper method %s defined in %s' %
                          (name, _DumpBaseClassList(bases_with_name)))
      if getattr(function, '_is_override', None):
        bases_with_name = [base for base in bases if hasattr(base, name)]
        if not bases_with_name:
          # TODO(pts): Report line numbers (elsewhere etc.).
          problems.append(
              '@override method %s not defined in %s' %
              (name, _DumpBaseClassList(bases)))
      # We don't need any special casing for getattr(..., '_is_final', None) below
      # if getattr(base, name) is an ``instancemethod'' created from a
      # classmethod or a function. This is because an instancemathod
      # automirorrs all attributes of its im_func.
      bases_with_final = []
      for base in bases:
        function = _UnwrapFunctionOrMethod(getattr(base, name, None))
        if getattr(function, '_is_final', None):
          bases_with_final.append(function._full_name)
      if bases_with_final:
        problems.append(
            'method %s overrides @final %s' %
            (name, _DumpMethodList(bases_with_final)))
      if function is dict_obj[name]:  # function is instance method
        bases_with_finalim = [
            base for base in bases if getattr(_UnwrapFunctionOrMethod(getattr(
                base, name, None)), '_is_finalim', None)]
        if bases_with_finalim:
          # !! Use base ._full_name like in @final.
          problems.append(
              'instance method %s overrides @finalim method in %s' %
              (name, _DumpBaseClassList(bases_with_finalim)))
  if abstract_methods:
    abstract_method_fullnames = set()
    for fullnames in abstract_methods.itervalues():
      abstract_method_fullnames.update(fullnames)
    # TODO(pts): Copy __doc__ etc.
    dict_obj['__init__'] = type(_AbstractInit)(
        _AbstractInit.func_code,
        {'__builtins__': _AbstractInit.func_globals['__builtins__'],
         '_abstract_method_fullnames': abstract_method_fullnames,
         '_DumpMethodList': _DumpMethodList,
         'BadInstantiation': BadInstantiation,
         '_orig_init': dict_obj.get('__init__'),
         '_has_orig_init': '__init__' in dict_obj},
        _AbstractInit.func_defaults,
        _AbstractInit.func_closure)
    if '__init__' in abstract_methods:
      init, _ = _UnwrapFunction(dict_obj['__init__'])
      init._is_abstract = True
      init._full_name = '%s.%s.__init__' % (module, class_name)
    # TODO(pts): can we optimize this for single inheritance, so that
    # _AbstractInit is called for only a few classes?
  if problems:
    msg = ['Cannot create ']
    if abstract_methods:
      msg.append('abstract class ')
    else:
      msg.append('class ')
    msg.append('%s.%s because ' % (module, class_name))
    msg.append('; '.join(problems))
    msg.append('.')
    raise BadClass(''.join(msg))
    

class _PObjectMeta(type):
  def __new__(cls, class_name, bases, dict_obj):
    # cls is _PObjectMeta here.
    # Call CheckDecorators before type.__new__ first, because
    # CheckDecorators may modify dict_obj.
    CheckDecorators(class_name, bases, dict_obj)
    kls = type.__new__(cls, class_name, bases, dict_obj)
    if _AbstractInit.func_code is getattr(
        kls.__dict__.get('__init__'), 'func_code', None):
      # This creates a circular reference, but that's unavoidable.
      kls.__init__.func_globals['_for_super_class'] = kls
    return kls
