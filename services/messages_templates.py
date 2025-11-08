"""Local fallback templates for Cyberherd.

These are used only when a shared template is not configured or unavailable.
They mirror keys used by make_messages() for simple formatting.
"""

# Integer-like keys mirror the original middleware style. Align local fallback
# names with shared extension categories for consistency.
cyber_herd_join = {
    0: "{name} has joined the ⚡ CyberHerd ⚡. {thanks_part} The feeder will activate in {difference} sats.\n\n https://lightning-goats.com\n\n",
}

thank_you_variations = {
    0: "Thank you for the contribution of {new_amount} sats.",
}

headbutt_success_dict = {
    0: "{attacker_name} headbutted {victim_name}! {attacker_amount} > {victim_amount}. Welcome to the herd.",
}

headbutt_info_dict = {
    0: "Headbutt attempt needs {required_sats} more sats to succeed against {victim_name}.",
}

member_increase = {
    0: "{member_name} increased contribution by {increase_amount} to {new_total} sats."
}

kind_6_repost = {
    0: "{member_name} reposted! Contribution increased by {increase_amount} to {new_total} sats. ⚡"
}

kind_7_reaction = {
    0: "{member_name} reacted! Contribution increased by {increase_amount} to {new_total} sats. ⚡"
}

# Maintain backward compatibility aliases (old variable names referenced elsewhere)
cyber_herd_dict = cyber_herd_join  # legacy alias
member_increase_dict = member_increase
kind_6_repost_dict = kind_6_repost
kind_7_reaction_dict = kind_7_reaction
