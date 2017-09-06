import datetime
from subprocess import check_output, CalledProcessError
from urlparse import urlparse

from celery.task import task
from django.conf import settings
from django.contrib.sites.models import Site
from django.core.urlresolvers import reverse
from django.utils.http import urlquote

from edx_ace import ace
from edx_ace.message import MessageType, Message
from edx_ace.recipient import Recipient
from edx_ace.utils.date import deserialize

from edxmako.shortcuts import marketing_link
from openedx.core.djangoapps.schedules.models import Schedule, ScheduleConfig


ROUTING_KEY = getattr(settings, 'ACE_ROUTING_KEY', None)


class RecurringNudge(MessageType):
    def __init__(self, day, *args, **kwargs):
        super(RecurringNudge, self).__init__(*args, **kwargs)
        self.name = "recurringnudge_day{}".format(day)


@task(ignore_result=True, routing_key=ROUTING_KEY)
def recurring_nudge_schedule_hour(
    site_id, day, target_hour_str, org_list, exclude_orgs=False, override_recipient_email=None,
):
    target_hour = deserialize(target_hour_str)
    msg_type = RecurringNudge(day)

    for (user, language, context) in _recurring_nudge_schedules_for_hour(target_hour, org_list, exclude_orgs):
        msg = msg_type.personalize(
            Recipient(
                user.username,
                override_recipient_email or user.email,
            ),
            language,
            context,
        )
        _recurring_nudge_schedule_send.apply_async((site_id, str(msg)), retry=False)


@task(ignore_result=True, routing_key=ROUTING_KEY)
def _recurring_nudge_schedule_send(site_id, msg_str):
    site = Site.objects.get(pk=site_id)
    if not ScheduleConfig.current(site).deliver_recurring_nudge:
        return

    msg = Message.from_string(msg_str)
    ace.send(msg)


def _recurring_nudge_schedules_for_hour(target_hour, org_list, exclude_orgs=False):
    schedules = Schedule.objects.select_related(
        'enrollment__user__profile',
        'enrollment__course',
    ).filter(
        start__gte=target_hour,
        start__lt=target_hour + datetime.timedelta(minutes=60),
        enrollment__is_active=True,
    )

    if org_list is not None:
        if exclude_orgs:
            schedules = schedules.exclude(enrollment__course__org__in=org_list)
        else:
            schedules = schedules.filter(enrollment__course__org__in=org_list)

    if "read_replica" in settings.DATABASES:
        schedules = schedules.using("read_replica")

    for schedule in schedules:
        enrollment = schedule.enrollment
        user = enrollment.user

        course_id_str = str(enrollment.course_id)
        course = enrollment.course

        course_root_relative_url = reverse('course_root', args=[course_id_str])
        dashboard_relative_url = reverse('dashboard')

        template_context = {
            'student_name': user.profile.name,
            'course_name': course.display_name,
            'course_url': absolute_url(course_root_relative_url),

            # Platform information
            'homepage_url': encode_url(marketing_link('ROOT')),
            'dashboard_url': absolute_url(dashboard_relative_url),
            'template_revision': get_scm_revision(),
            'template_tag': get_scm_tag(),
            'platform_name': settings.PLATFORM_NAME,
            'contact_mailing_address': settings.CONTACT_MAILING_ADDRESS,
            'social_media_urls': encode_urls_in_dict(getattr(settings, 'SOCIAL_MEDIA_FOOTER_URLS', {})),
            'mobile_store_urls': encode_urls_in_dict(getattr(settings, 'MOBILE_STORE_URLS', {})),

            # This is used by the bulk email optout policy
            'course_id': course_id_str,
        }

        yield (user, course.language, template_context)


def get_scm_revision():
    try:
        return check_output(['git', 'log', '-1', '--format=%H']).strip()
    except CalledProcessError:
        return ''


def get_scm_tag():
    try:
        return check_output(['git', 'describe', '--always', '--tags']).strip()
    except CalledProcessError:
        return ''


def encode_url(url):
    # Sailthru has a bug where URLs that contain "+" characters in their path components are misinterpreted
    # when GA instrumentation is enabled. We need to percent-encode the path segments of all URLs that are
    # injected into our templates to work around this issue.
    parsed_url = urlparse(url)
    modified_url = parsed_url._replace(path=urlquote(parsed_url.path))
    return modified_url.geturl()


def absolute_url(relative_path):
    return encode_url(u'{}{}'.format(settings.LMS_ROOT_URL, relative_path))


def encode_urls_in_dict(mapping):
    urls = {}
    for key, value in mapping.iteritems():
        urls[key] = encode_url(value)
    return urls
