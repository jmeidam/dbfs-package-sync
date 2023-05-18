from requests.exceptions import HTTPError


class DatabricksApiError(Exception):
    """Error messages from the Databricks API are consistently formatted, but are hard to read when returned
    as a single string.

    This Error class parses the API HTTPError messages to raise a clear message.

    :param http_error:
        HTTPError instance
    :param message_prefix:
        A message to print before HTTPError details
    :param include:
        A list of api error details to print in the message
        This can be 'error_code', 'message', 'url', 'reason', 'status_code'
        Default is 'error_code', which will be something like "RESOURCE_ALREADY_EXISTS" and 'message'.
    """

    def __init__(
        self, http_error: HTTPError, message_prefix: str = "Databricks API raised an HTTPError", include: list = None
    ):
        self.http_error = http_error
        self.message_prefix = message_prefix
        self.message = None
        self._allowed = {"error_code", "message", "url", "reason", "status_code"}

        if not include:
            self.include = ["error_code", "message"]

        if not set(self.include).issubset(self._allowed):
            raise ValueError(f"Invalid include item. Allowed: {self._allowed}")

        self.api_response_json = self.http_error.response.json()

        self._create_message()

        super().__init__(self.message)

    def _create_message(self):
        message = f"{self.message_prefix}:\n"
        for key in self.include:
            if key in ("error_code", "message"):
                val = self.api_response_json[key]
            else:
                val = getattr(self.http_error.response, key)
            message += f"    {key}: {val}\n"
        self.message = message


class DatabricksConfigError(Exception):
    pass
