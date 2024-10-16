import json
import hashlib
import random
from datetime import datetime, timedelta
from faker import Faker


# def generate_session_id():
#     return 'SESSION-' + hashlib.sha256(datetime.now().isoformat().encode()).hexdigest()[-6:]
#
#
# def generate_user_id():
#     return 'UUID-' + hashlib.sha256(datetime.now().isoformat().encode()).hexdigest()[:6]


def generate_page_url():
    article_categories = ['technology', 'science', 'health', 'nature', 'transport',
                          'engineering', 'education', 'sport', 'medicine', 'news']
    pages = [
        "home",
        "about",
        "services",
        "contacts",
        f"article/{random.choice(article_categories)}/{random.randint(1, 1000)}"
    ]
    weights = [0.27, 0.015, 0.1, 0.015, 0.60]  # Visits probability for pages
    return random.choices(pages, weights=weights, k=1)[0]


def generate_notification():
    notifications = [
        "System maintenance scheduled for tonight.",
        "New features added to the service.",
        "Update available.",
        "Our privacy policy has been updated. Please review the changes.",
        "Suspicious activity detected on your account. Please review.",
        "Please take a moment to rate our service."
    ]
    return random.choice(notifications)


def generate_comment():
    return Faker().sentence(nb_words=random.randint(5, 15))


def simulate_user_session(num_interacted_pages, num_actions, session_id, user_id):
    # session_id = generate_session_id()
    # user_id = generate_user_id()
    session_start_time = datetime.now()

    # Start session
    session = {
        'session_id': session_id,
        'user_id': user_id,
        'session_start': session_start_time.isoformat(),
        'history': [],
        'session_end': None
    }

    # track articles interacted with
    interacted_articles = {}

    system_notification_count = 0
    max_system_notifications = 3

    previous_page_id = None
    current_time = session_start_time

    # user actions simulation
    for _ in range(num_interacted_pages):
        page_id = generate_page_url()

        while page_id == previous_page_id:
            page_id = generate_page_url()  # gen new page_url if prev the same

        page_data = {
            'page_id': page_id,
            'actions': []
        }

        if page_id.startswith("article/"):
            page_data['article_category'] = page_id.split('/')[1]

        # Initial scroll depth
        current_scroll_depth = random.randint(0, 10)
        time_gap = timedelta(seconds=random.randint(5, 200))
        current_time += time_gap

        # Add page_view as the first action
        page_data['actions'].append({
            'event_type': 'page_view',
            'timestamp': current_time.isoformat(),
            'scroll_depth': current_scroll_depth
        })

        previous_page_id = page_id  # update previous page_id next add page_view

        if page_id.startswith("article/"):
            if page_id not in interacted_articles:
                interacted_articles[page_id] = {
                    'like': False,
                    'comment': None,
                    'save': False
                }

            for _ in range(num_actions):
                time_gap = timedelta(seconds=random.randint(5, 620))
                current_time += time_gap
                event_type = random.choices(['like', 'share', 'comment', 'save', 'ad_click'],
                                            weights=[0.4, 0.2, 0.16, 0.17, 0.07], k=1)[0]
                current_scroll_depth = min(current_scroll_depth + random.randint(10, 30), 100)

                if event_type == 'like' and not interacted_articles[page_id]['like']:
                    page_data['actions'].append({
                        'event_type': 'like',
                        'timestamp': current_time.isoformat(),
                        'scroll_depth': current_scroll_depth
                    })
                    interacted_articles[page_id]['like'] = True
                    continue

                elif event_type == 'comment' and current_scroll_depth >= 80:
                    if interacted_articles[page_id]['comment'] is None:
                        comment_text = generate_comment()
                        page_data['actions'].append({
                            'event_type': 'comment',
                            'timestamp': current_time.isoformat(),
                            'scroll_depth': current_scroll_depth,
                            'text': comment_text
                        })
                        interacted_articles[page_id]['comment'] = comment_text
                    else:
                        updated_comment_text = generate_comment()
                        page_data['actions'].append({
                            'event_type': 'comment_update',
                            'timestamp': current_time.isoformat(),
                            'scroll_depth': current_scroll_depth,
                            'text': updated_comment_text
                        })
                        interacted_articles[page_id]['comment'] = updated_comment_text
                    continue

                elif event_type == 'save' and not interacted_articles[page_id]['save']:
                    page_data['actions'].append({
                        'event_type': 'save',
                        'timestamp': current_time.isoformat(),
                        'scroll_depth': current_scroll_depth
                    })
                    interacted_articles[page_id]['save'] = True
                    continue

                elif event_type == 'share':
                    page_data['actions'].append({
                        'event_type': 'share',
                        'timestamp': current_time.isoformat(),
                        'scroll_depth': current_scroll_depth
                    })
                    continue

                elif event_type == 'ad_click':
                    page_data['actions'].append({
                        'event_type': 'ad_click',
                        'timestamp': current_time.isoformat(),
                        'scroll_depth': current_scroll_depth,
                        'ad_id': random.randint(1, 10)
                    })
                    continue

        else:
            for _ in range(random.randint(1, 3)):
                time_gap = timedelta(seconds=random.randint(5, 180))
                current_time += time_gap
                event_type = random.choices(['page_view', 'system_notification'], weights=[0.85, 0.15], k=1)[0]
                current_scroll_depth = min(current_scroll_depth + random.randint(5, 20), 100)

                if page_id in ["about", "contacts"] and event_type == 'system_notification':
                    continue

                if event_type == 'system_notification' and system_notification_count < max_system_notifications:
                    page_data['actions'].append({
                        'event_type': 'system_notification',
                        'timestamp': current_time.isoformat(),
                        'scroll_depth': current_scroll_depth,
                        'text': generate_notification()
                    })
                    system_notification_count += 1
                else:
                    page_data['actions'].append({
                        'event_type': event_type,
                        'timestamp': current_time.isoformat(),
                        'scroll_depth': current_scroll_depth
                    })

        # Add page to history
        session['history'].append(page_data)

    # End session
    session['session_end'] = current_time.isoformat()

    return json.dumps(session, indent=4)


if __name__ == "__main__":
    # print(generate_page_url())
    print(simulate_user_session(num_interacted_pages=random.randint(1, 10),
                                num_actions=random.randint(3, 15)))

