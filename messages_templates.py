"""Compact set of message templates ported from the middleware messages module.

This file intentionally contains a reduced, focused set of templates used by the
extension's messaging adapter. Keep these templates small and easy to maintain.
"""

"""Compact set of message templates ported from the middleware messages module.

This file intentionally contains a reduced, focused set of templates used by the
extension's messaging adapter. Keep these templates small and easy to maintain.
"""


cyber_herd: dict[int, str] = {
    0: (
        "{name} has joined the ⚡ CyberHerd ⚡. {thanks_part} The feeder will activate"
        " in {difference} sats.\n\n https://lightning-goats.com\n\n"
    ),
    1: (
        "Welcome, {name}. {thanks_part} The ⚡ CyberHerd ⚡ grows. {difference} sats"
        " are required for the next feeding cycle.\n\n"
        " https://lightning-goats.com\n\n"
    ),
}

thank_you_variations: list[str] = [
    "Thank you for the contribution of {new_amount} sats.",
    "Your {new_amount} sat contribution has been received and supports the herd.",
    "We have received your contribution of {new_amount} sats.",
]

cyber_herd_treats: dict[int, str] = {
    0: (
        "{name} has received a reward of {new_amount} sats from the ⚡ CyberHerd ⚡"
        " distribution.\n\n https://lightning-goats.com\n\n"
    ),
}

member_increase: dict[int, str] = {
    0: (
        "⚡CyberHerd⚡: {member_name} has increased their contribution by {increase_amount} sats,"
        " bringing their total to {new_total}. Biological fact: Goats are ruminants with a"
        " four-chambered stomach, allowing them to efficiently digest fibrous plants.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    1: (
        "⚡CyberHerd⚡: With an additional {increase_amount} sats, {member_name}'s new total is"
        " {new_total}. Did you know? The rectangular pupils of a goat provide a wide,"
        " 320-340 degree field of vision, aiding in predator detection.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    2: (
        "⚡CyberHerd⚡: {member_name} adds {increase_amount} sats, for a total of {new_total}."
        " Historical fact: Goats were among the first animals to be domesticated by humans,"
        " approximately 10,000 years ago.\n\n https://lightning-goats.com\n\n"
    ),
    3: (
        "⚡CyberHerd⚡: The fund grows as {member_name} contributes {increase_amount} more sats,"
        " reaching {new_total}. Social observation: Goats are herd animals and can become"
        " depressed if kept in isolation.\n\n https://lightning-goats.com\n\n"
    ),
    4: (
        "⚡CyberHerd⚡: {member_name} has raised their contribution to {new_total} sats with an"
        " added {increase_amount}. Anatomical fact: Goats use their prehensile lips to be"
        " selective eaters, often choosing the most nutritious parts of a plant.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    5: (
        "⚡CyberHerd⚡: {member_name}'s contribution has grown to {new_total} sats with"
        " {increase_amount} more. Did you know? Different goat breeds produce unique fibers,"
        " such as cashmere from Cashmere goats and mohair from Angora goats.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    6: (
        "⚡CyberHerd⚡: {member_name} adds {increase_amount} sats to the herd, for a new total of"
        " {new_total}. Agility fact: Goats are excellent climbers, with hooves adapted for"
        " gripping steep and rocky terrain.\n\n https://lightning-goats.com\n\n"
    ),
    7: (
        "⚡CyberHerd⚡: The total from {member_name} is now {new_total} sats after contributing"
        " another {increase_amount}. Cognitive fact: Studies have shown that goats can"
        " differentiate between human facial expressions and prefer happy faces.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    8: (
        "⚡CyberHerd⚡: {member_name} contributes {increase_amount} more, bringing their total to"
        " {new_total}. Health fact: Goat milk is often considered easier to digest than cow's"
        " milk because it has smaller fat globules and is naturally homogenized.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    9: (
        "⚡CyberHerd⚡: A contribution of {increase_amount} sats from {member_name} increases their"
        " total to {new_total}. Did you know? 'Fainting' goats have a genetic condition called"
        " myotonia congenita, which causes their muscles to stiffen when startled.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    10: (
        "⚡CyberHerd⚡: {member_name} has increased their total to {new_total} sats by adding"
        " {increase_amount}. Environmental fact: Goats are effective browsers and are often"
        " used for land management to clear brush and control invasive plant species.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    11: (
        "⚡CyberHerd⚡: {member_name} adds another {increase_amount} sats, reaching a total of"
        " {new_total}. Anatomical fact: Unlike sheep, the tails of most goat breeds point"
        " upwards unless the goat is sick or distressed.\n\n https://lightning-goats.com\n\n"
    ),
    12: (
        "⚡CyberHerd⚡: {member_name} now has a total of {new_total} sats contributed after adding"
        " {increase_amount}. Did you know? A male goat is called a 'buck' or 'billy,' a female"
        " is a 'doe' or 'nanny,' and a young goat is a 'kid.'\n\n https://lightning-goats.com\n\n"
    ),
    13: (
        "⚡CyberHerd⚡: The contribution from {member_name} grows by {increase_amount} sats to"
        " {new_total}. Historical legend: Coffee was supposedly discovered when an Ethiopian"
        " goat herder noticed his goats became energetic after eating coffee cherries.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    14: (
        "⚡CyberHerd⚡: {member_name} adds {increase_amount} more sats, for a new total of"
        " {new_total}. Dietary fact: Goats are selective feeders and will often refuse to eat"
        " hay that is soiled or has been trampled on.\n\n https://lightning-goats.com\n\n"
    ),
    15: (
        "⚡CyberHerd⚡: {member_name}'s total contribution is now {new_total} sats after an"
        " increase of {increase_amount}. Did you know? Goats do not have teeth on their upper"
        " front jaw; instead, they have a hard dental pad.\n\n https://lightning-goats.com\n\n"
    ),
    16: (
        "⚡CyberHerd⚡: {member_name} has increased their support with {increase_amount} more sats,"
        " reaching {new_total}. Global fact: More people worldwide consume goat milk than cow's"
        " milk.\n\n https://lightning-goats.com\n\n"
    ),
    17: (
        "⚡CyberHerd⚡: {member_name} boosts their contribution by {increase_amount} sats, for a"
        " total of {new_total}. Social fact: Mother goats will often call to their kids to"
        " ensure they remain close by in the herd.\n\n https://lightning-goats.com\n\n"
    ),
    18: (
        "⚡CyberHerd⚡: {member_name} contributes {increase_amount} sats, bringing their total to"
        " {new_total}. Did you know? Goats can be taught their name and to come when called.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    19: (
        "⚡CyberHerd⚡: The total from {member_name} has grown to {new_total} sats with an"
        " additional {increase_amount}. Anatomical fact: A goat's horns are made of living bone"
        " surrounded by keratin and are used for defense, dominance, and thermoregulation.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    20: (
        "⚡CyberHerd⚡: {member_name}'s new total is {new_total} sats after a contribution of"
        " {increase_amount}. Behavioral fact: Goats are playful animals, especially when young,"
        " and engage in activities like climbing and jumping for enjoyment.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    21: (
        "⚡CyberHerd⚡: An increase of {increase_amount} sats brings {member_name}'s total to"
        " {new_total}. Did you know? There are over 210 breeds of goats in the world.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    22: (
        "⚡CyberHerd⚡: {member_name}'s contribution total is now {new_total} after adding"
        " {increase_amount} sats. Did you know? Goats' wool is highly prized in the textile"
        " industry, with certain breeds producing fibers like mohair and cashmere.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    23: (
        "⚡CyberHerd⚡: With an additional {increase_amount} sats, {member_name} has a new total of"
        " {new_total}. Sensory fact: Goats have an excellent sense of smell, which they use to"
        " find food and recognize other goats.\n\n https://lightning-goats.com\n\n"
    ),
    24: (
        "⚡CyberHerd⚡: {member_name} has added {increase_amount} sats, bringing their contribution"
        " to {new_total}. Did you know? Genetically engineered goats can produce spider silk"
        " protein in their milk, a result of advanced genetic engineering techniques.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    25: (
        "⚡CyberHerd⚡: The total from {member_name} is now {new_total} sats after contributing"
        " {increase_amount} more. Health fact: Goat meat is leaner and has less cholesterol than"
        " beef, pork, or even chicken.\n\n https://lightning-goats.com\n\n"
    ),
    26: (
        "⚡CyberHerd⚡: {member_name} adds {increase_amount} sats to their total, which is now"
        " {new_total}. Did you know? A goat giving birth is said to be 'kidding.'\n\n"
        " https://lightning-goats.com\n\n"
    ),
    27: (
        "⚡CyberHerd⚡: {member_name}'s contribution now stands at {new_total} sats after adding"
        " {increase_amount}. Did you know? The lifespan of a domestic goat is typically between"
        " 15 and 18 years.\n\n https://lightning-goats.com\n\n"
    ),
    28: (
        "⚡CyberHerd⚡: With another {increase_amount} sats, {member_name}'s total reaches"
        " {new_total}. Social fact: Mother goats will often call to their kids to ensure they"
        " remain close by in the herd.\n\n https://lightning-goats.com\n\n"
    ),
    29: (
        "⚡CyberHerd⚡: {member_name} adds {increase_amount} sats to their total, for a total of"
        " {new_total}. Did you know? The term 'scapegoat' originates from an ancient Hebrew"
        " tradition involving goats.\n\n https://lightning-goats.com\n\n"
    ),
    30: (
        "⚡CyberHerd⚡: The total from {member_name} has grown to {new_total} sats with an"
        " additional {increase_amount}. Anatomical fact: A goat's horns are made of living bone"
        " surrounded by keratin and are used for defense, dominance, and thermoregulation.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    31: (
        "⚡CyberHerd⚡: {member_name}'s new total is {new_total} sats after a contribution of"
        " {increase_amount}. Behavioral fact: Goats are playful animals, especially when young,"
        " and engage in activities like climbing and jumping for enjoyment.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    32: (
        "⚡CyberHerd⚡: An increase of {increase_amount} sats brings {member_name}'s total to"
        " {new_total}. Did you know? There are over 210 breeds of goats in the world.\n\n"
        " https://lightning-goats.com\n\n"
    ),
    33: (
        "⚡CyberHerd⚡: {member_name}'s contribution total is now {new_total} after adding"
        " {increase_amount} sats. Sensory fact: Goats have an excellent sense of smell, which"
        " they use to find food and recognize other goats.\n\n https://lightning-goats.com\n\n"
    ),
    34: (
        "⚡CyberHerd⚡: With an additional {increase_amount} sats, {member_name} has a new total of"
        " {new_total}. Did you know? Genetically engineered goats can produce spider silk"
        " protein in their milk.\n\n https://lightning-goats.com\n\n"
    ),
    35: (
        "⚡CyberHerd⚡: {member_name} has added {increase_amount} sats, bringing their contribution"
        " to {new_total}. Ecological fact: In permaculture systems, goats are valued for their"
        " ability to clear land and provide manure for fertilizer.\n\n https://lightning-goats.com\n\n"
    ),
    36: (
        "⚡CyberHerd⚡: The total from {member_name} is now {new_total} sats after contributing"
        " {increase_amount} more. Health fact: Goat meat is leaner and has less cholesterol than"
        " beef, pork, or even chicken.\n\n https://lightning-goats.com\n\n"
    ),
    37: (
        "⚡CyberHerd⚡: {member_name} boosts their contribution by {increase_amount} sats, for a"
        " total of {new_total}. Did you know? A goat giving birth is said to be 'kidding.'\n\n"
        " https://lightning-goats.com\n\n"
    ),
    38: (
        "⚡CyberHerd⚡: {member_name}'s contribution now stands at {new_total} sats after adding"
        " {increase_amount}. Vision fact: The unique shape of their pupils gives goats good night"
        " vision.\n\n https://lightning-goats.com\n\n"
    ),
    39: (
        "⚡CyberHerd⚡: {member_name} has increased their support with another {increase_amount}"
        " sats, for a total of {new_total}. Agricultural fact: Goats play a vital role in"
        " sustainable agriculture by managing weeds without the need for chemical herbicides.\n\n"
        " https://lightning-goats.com\n\n"
    ),
}

headbutt_info: dict[int, str] = {
    0: (
        "⚡headbutt⚡: The ⚡ CyberHerd ⚡ is currently at full capacity. To join, a"
        " contribution of {required_sats} sats is needed to displace the member with"
        " the lowest contribution, {victim_name}.\n\n"
        " https://lightning-goats.com\n\n"
    ),
}

headbutt_success: dict[int, str] = {
    0: (
        "⚡headbutt⚡: A new member has joined the ⚡ CyberHerd ⚡. {attacker_name}"
        " ({attacker_amount} sats) has displaced {victim_name} ({victim_amount}"
        " sats).\n\n https://lightning-goats.com\n\n"
    ),
}

feeder_trigger: dict[int, str] = {
    0: (
        "Feeder Trigger Alert! {new_amount} sats added. Goats, like {goat_name}, have"
        " a remarkable digestive system with four chambers, which helps them break"
        " down tough plant material.\n\n https://lightning-goats.com\n\n"
    ),
}

variations: dict[int, str] = {
    0: "{difference} sats are required for feeder activation.",
    1: "The next feeding cycle will begin in {difference} sats.",
}

__all__ = [
    "cyber_herd",
    "cyber_herd_treats",
    "feeder_trigger",
    "headbutt_info",
    "headbutt_success",
    "member_increase",
    "feeding_regular",
    "feeding_bonus",
    "feeding_remainder",
    "feeding_fallback",
    "thank_you_variations",
    "variations",
]
