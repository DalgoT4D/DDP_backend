"""
Custom Pylint plugin for Django models.
This helps pylint understand Django's runtime-added attributes.
"""

from pylint.checkers import BaseChecker
from astroid import MANAGER, nodes, inference_tip
from astroid.builder import AstroidBuilder
import astroid


def register(linter):
    """Required method to auto register plugin."""
    linter.register_checker(DjangoModelChecker(linter))


def transform_model(node):
    """Add objects and DoesNotExist to Django model classes."""
    if node.name == 'Model':
        return

    # Create objects manager
    manager_code = """
    class Manager:
        def get(self, *args, **kwargs): pass
        def filter(self, *args, **kwargs): pass
        def exclude(self, *args, **kwargs): pass
        def create(self, *args, **kwargs): pass
        def all(self): pass
        def count(self): pass
        def order_by(self, *args, **kwargs): pass
        def select_related(self, *args, **kwargs): pass
        def prefetch_related(self, *args, **kwargs): pass
    objects = Manager()
    """
    
    # Create DoesNotExist exception
    does_not_exist_code = """
    class DoesNotExist(Exception): pass
    DoesNotExist = DoesNotExist()
    """
    
    # Build and assign the objects manager
    fake = AstroidBuilder(MANAGER).string_build(manager_code)
    node.locals['objects'] = fake['objects']
    
    # Build and assign DoesNotExist
    fake = AstroidBuilder(MANAGER).string_build(does_not_exist_code)
    node.locals['DoesNotExist'] = fake['DoesNotExist']


class DjangoModelChecker(BaseChecker):
    """Checker for Django Model attributes."""
    
    name = 'django-model'
    priority = -1
    msgs = {
        'W0001': (
            'Django model has no attribute %r',
            'django-model-no-attribute',
            'Used when an invalid attribute is accessed in a Django model'
        ),
    }
    
    def __init__(self, linter=None):
        super().__init__(linter)
        # Register transforms
        MANAGER.register_transform(
            nodes.ClassDef,
            self._transform_model,
            lambda node: any(base.qname() == 'django.db.models.base.Model' 
                           for base in node.bases)
        )
    
    def _transform_model(self, node):
        """Transform Django model classes."""
        transform_model(node)
        return node 