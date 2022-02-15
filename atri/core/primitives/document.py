# Copyright 2020 Marcos Pontes. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY MARCOS PONTES ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL MARCOS PONTES OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of MARCOS PONTES.

"""
Defines a core primitive structure of atri search: documents.
"""

from typing import Dict, AnyStr


class ADoc:
    """
    Base class of Atri that represents an indexable document.
    A document can be interpreted as an in-memory representation of something indexable by the atri engine.
    Essentially, document can be files of any format, since it has the following properties:
    1. a textual title that synthesizes the document's content (fogged titles are unpleasant).
    2. a textual body that represents the whole document's content.
    """

    def __init__(self, name: str, **fields):
        """
        Initializes a new instance of the ADoc class.
        :param name: Name of the document.
        :param fields: Key-value pairs of fields.
        """
        self._name = name
        self._fields = fields

    @property
    def name(self) -> str:
        """
        Gets the name of the document.
        :return: The name of the document.
        """
        return self._name

    @name.setter
    def name(self, value: str):
        """
        Sets the name of the document.
        :param value: The name of the document.
        """
        self._name = value

    @property
    def fields(self) -> Dict[str, AnyStr]:
        """
        Gets the fields of the document.
        :return: The fields of the document.
        """
        return self._fields

    @fields.setter
    def fields(self, value: Dict[str, AnyStr]):
        """
        Sets the fields of the document.
        :param value: The fields of the document.
        """
        self._fields = value

    def add(self, field_name: str, field_value: AnyStr):
        """
        Adds a field to the document.
        :param field_name: The name of the field.
        :param field_value: The value of the field.
        """
        self._fields[field_name] = field_value

    def json(self) -> Dict[str, AnyStr]:
        """
        Gets the document as a JSON object.
        :return: The document as a JSON object.
        """
        return {
            'name': self._name,
            'fields': [
                {
                    field_name: field_value.decode(encoding='utf-8') if isinstance(field_value, bytes) else field_value
                }
                for field_name, field_value in self._fields.items()
            ]
        }
