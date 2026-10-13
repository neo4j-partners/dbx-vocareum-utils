"""URL shapes for the Vocareum REST API, shared by every command-line tool.

Held in one place because all three tools address the same resources and a
wrong path is answered with a bare HTTP 400 that names nothing, so a URL built
two slightly different ways in two files is an expensive kind of drift.
"""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass

DEFAULT_API_BASE = "https://api.vocareum.com/api/v2"


@dataclass(frozen=True)
class PartEndpoints:
    """The course, assignment, and part coordinates every tool addresses."""

    api_base: str
    course_id: str
    assignment_id: str
    part_id: str

    def part_url(self) -> str:
        """Return the part resource URL, which is also the upload endpoint."""
        return (
            f"{self.api_base}/courses/{self.course_id}"
            f"/assignments/{self.assignment_id}"
            f"/parts/{self.part_id}"
        )

    def files_url(self, query: dict[str, str]) -> str:
        """Return the files URL for a query mapping."""
        return f"{self.part_url()}/files?{urllib.parse.urlencode(query)}"

    def transaction_url(self, transaction_id: str) -> str:
        """Return the transaction status URL for a transaction id."""
        return f"{self.api_base}/transaction/{transaction_id}"


@dataclass(frozen=True)
class OrgEndpoints:
    """The organization-scoped paths, which are not reachable per course.

    Held apart from the part coordinates because the organization number is a
    different identifier from anything on a course, and because these paths are
    optional: a run without one still reads everything else.
    """

    api_base: str
    org_number: str

    def databricks_workspaces_url(self) -> str:
        """Return the URL listing the Databricks workspaces Vocareum has built.

        The org path is the only shape that answers. ``/databricks_workspaces``,
        the course-scoped form, and the part-scoped form all return a bare
        ``400 Invalid Request``, so there is no way to ask this question about
        one part without knowing the organization number.
        """
        return f"{self.api_base}/orgs/{self.org_number}/databricks_workspaces"


@dataclass(frozen=True)
class SessionEndpoints(PartEndpoints):
    """A part addressed for one student, which is where a lab session lives."""

    user_id: str

    def resource_url(self) -> str:
        """Return the session resource URL for this student.

        The user id is the last path segment, not a ``/users/{id}`` collection.
        Vocareum answers any other shape with a bare HTTP 400 "Invalid
        Request", which names nothing.
        """
        return f"{self.part_url()}/resources/{self.user_id}"
